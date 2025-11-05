//TotalJobs scraper - Production-ready implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// Selector documentation based on actualTotalJobs.com structure (Nov 2025)
// LISTING PAGE: Job links found as: <a href="/job/[title]/[company]-job[id]">
// Pattern: Links start with /job/ and contain job title, company slug, and job ID
// Pagination: <a href="/jobs/admin?page=2"> (numbered pages 1-285+)
// Job cards: Each job is a heading with company/location/salary below
// DETAIL PAGE: May require authentication; JSON-LD structured data preferred
// Fallback selectors for title, company, location, salary, description

// Utility: pick first non-empty text from multiple selectors
function pickText($el, selectors) {
  for (const sel of selectors) {
    try {
      const node = typeof sel === 'string' ? $el.find(sel) : sel;
      if (node && node.length) {
        const t = node.text().trim();
        if (t) return t;
      }
    } catch (e) { /* skip */ }
  }
  return null;
}

// Utility: pick first non-empty attribute from multiple selectors
function pickAttr($el, selectors, attr) {
  for (const sel of selectors) {
    try {
      const node = typeof sel === 'string' ? $el.find(sel) : sel;
      if (node && node.length) {
        const a = node.attr(attr);
        if (a) return a.trim();
      }
    } catch (e) { /* skip */ }
  }
  return null;
}

// Utility: extract JSON-LD structured data for JobPosting
function extractJsonLd($, url) {
  const scripts = $('script[type="application/ld+json"]');
  for (let i = 0; i < scripts.length; i++) {
    try {
      const rawJson = $(scripts[i]).html();
      if (!rawJson) continue;
      const parsed = JSON.parse(rawJson);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const item of items) {
        if (!item) continue;
        const type = item['@type'] || item.type;
        if (type === 'JobPosting' || (Array.isArray(type) && type.includes('JobPosting'))) {
          return {
            title: item.title || item.name || null,
            company: item.hiringOrganization?.name || null,
            location: item.jobLocation?.address?.addressLocality || item.jobLocation?.address?.addressRegion || null,
            date_posted: item.datePosted || null,
            description_html: item.description || null,
            salary: item.baseSalary?.value?.value || item.baseSalary?.value || null,
            job_type: item.employmentType || null,
          };
        }
      }
    } catch (e) {
      log.debug(`JSON-LD parse error on ${url}: ${e.message}`);
    }
  }
  return null;
}

// Utility: random delay for human-like browsing (stealth)
function randomDelay(min, max) {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Utility: exponential backoff for retries
function getBackoffDelay(retryCount) {
  return Math.min(1000 * Math.pow(2, retryCount) + Math.random() * 1000, 10000);
}

function normalizeDelayRange(range, fallback) {
  if (Array.isArray(range) && range.length === 2) {
    const [rawMin, rawMax] = range.map((val) => Number(val));
    if (Number.isFinite(rawMin) && Number.isFinite(rawMax)) {
      const min = Math.max(0, Math.min(rawMin, rawMax));
      const max = Math.max(min, Math.max(rawMin, rawMax));
      return { min, max };
    }
  }
  return fallback;
}

// Single-entrypoint main
await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = 'admin',
            location = '',
            category = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 10,
            collectDetails = true,
            startUrl,
            url,
            proxyConfiguration,
            postedWithin,
            maxConcurrency: inputMaxConcurrency,
            minConcurrency: inputMinConcurrency,
            maxRequestsPerMinute: inputMaxRpm,
            navigationDelayRange,
            listingDelayRange,
            detailDelayRange,
            blockDelayRange,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;

        const maxRequestsPerMinute = Number.isFinite(+inputMaxRpm) ? Math.max(90, +inputMaxRpm) : 180;
        const maxConcurrency = Number.isFinite(+inputMaxConcurrency) ? Math.max(4, Math.min(24, +inputMaxConcurrency)) : 10;
        const minConcurrency = Number.isFinite(+inputMinConcurrency)
            ? Math.max(2, Math.min(+inputMinConcurrency, maxConcurrency))
            : Math.max(4, Math.min(6, Math.floor(maxConcurrency * 0.6)));

        const navDelay = normalizeDelayRange(navigationDelayRange, { min: 50, max: 200 });
        const listDelay = normalizeDelayRange(listingDelayRange, { min: 50, max: 150 });
        const detailDelay = normalizeDelayRange(detailDelayRange, { min: 100, max: 250 });
        const blockDelay = normalizeDelayRange(blockDelayRange, { min: 1500, max: 3000 });

        const sessionPoolSize = Math.max(30, maxConcurrency * 3);

        // Build start URL from keyword/location or use provided URL
        const buildStartUrl = (kw, loc, cat, posted) => {
            const base = 'https://www.totaljobs.com/jobs';
            if (!kw && !loc && !cat) return `${base}/admin?page=1`;
            const u = new URL(base + (kw ? `/${encodeURIComponent(kw)}` : ''));
            if (loc) u.searchParams.set('Location', loc);
            if (cat) u.searchParams.set('Category', cat);
            if (posted && ['1', '3', '7'].includes(posted)) u.searchParams.set('postedWithin', posted);
            u.searchParams.set('page', '1');
            return u.href;
        };

        const initial = [];
        if (startUrl) initial.push(startUrl);
        else if (url) initial.push(url);
        else initial.push(buildStartUrl(keyword, location, category, postedWithin));

        log.info(`TotalJobs scraper started with ${initial.length} start URL(s)`);
        log.info(`Target: ${RESULTS_WANTED} jobs, max ${MAX_PAGES} pages, collectDetails: ${collectDetails}`);
        log.info(`Concurrency window: ${minConcurrency}-${maxConcurrency}, RPM limit: ${maxRequestsPerMinute}`);

        // Proxy configuration
        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : undefined;
        
        if (proxyConf) {
            log.info('✓ Proxy configuration enabled for anti-blocking');
        } else {
            log.warning('⚠ Running without proxies - may face rate limiting. Consider enabling Apify proxies for better results.');
        }

        let saved = 0;
        let pagesVisited = 0;
        const seenUrls = new Set();
        const failedUrls = new Set();

        await Dataset.open('totaljobs-jobs');

        // Stealth best practices: balanced speed and stealth
        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 4,
            useSessionPool: true,
            persistCookiesPerSession: true,
            sessionPoolOptions: {
                maxPoolSize: sessionPoolSize,
                sessionOptions: {
                    maxUsageCount: 20,
                    maxErrorScore: 5,
                },
            },
            maxConcurrency,
            minConcurrency,
            requestHandlerTimeoutSecs: 90,
            navigationTimeoutSecs: 60,
            maxRequestsPerMinute,
            ignoreSslErrors: true,
            suggestResponseEncoding: 'utf-8',
            
            // Pre-navigation hook for stealth headers
            preNavigationHooks: [
                async ({ request }, gotoOptions) => {
                    // Realistic referer
                    const referer = request.userData?.referer || 'https://www.google.com/';
                    
                    if (!gotoOptions.headers) gotoOptions.headers = {};
                    Object.assign(gotoOptions.headers, {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Referer': referer,
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': referer.includes('totaljobs') ? 'same-origin' : 'cross-site',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                    });

                    // Force HTTP/1.1 because TotalJobs intermittently closes HTTP/2 streams
                    gotoOptions.http2 = false;
                    
                    // Small network delay
                    await randomDelay(navDelay.min, navDelay.max);
                },
            ],

            async requestHandler({ request, $, enqueueLinks, session, log: crawlerLog }) {
                const isDetailPage = /\/job\/[^/]+\/[^/]+-job\d+/.test(request.url);
                const isListPage = /\/jobs\//.test(request.url) && !isDetailPage;

                // Skip if we've already reached our goal
                if (saved >= RESULTS_WANTED) {
                    crawlerLog.debug('Results target reached, skipping');
                    return;
                }

                // LIST PAGE: extract job links and pagination
                if (isListPage) {
                    pagesVisited++;
                    if (pagesVisited > MAX_PAGES) {
                        crawlerLog.info(`Reached max pages limit (${MAX_PAGES})`);
                        return;
                    }

                    // Find all job links: /job/[title]/[company]-job[id]
                    const jobLinks = [];
                    $('a[href^="/job/"]').each((i, el) => {
                        const href = $(el).attr('href');
                        if (href && href.match(/\/job\/[^/]+\/[^/]+-job\d+/)) {
                            const fullUrl = `https://www.totaljobs.com${href}`;
                            if (!seenUrls.has(fullUrl) && !failedUrls.has(fullUrl)) {
                                seenUrls.add(fullUrl);
                                
                                // Extract basic info from listing (fallback if detail fails)
                                const $link = $(el);
                                const title = $link.text().trim() || null;
                                
                                // Find parent container for company/location/salary
                                const $container = $link.closest('article, li, div').length 
                                    ? $link.closest('article, li, div') 
                                    : $link.parent();
                                
                                const company = pickText($container, [
                                    'a[href*="/jobs/"]',
                                    '.company',
                                    'span:contains("Ltd")',
                                ]) || null;
                                
                                const location = pickText($container, [
                                    'span:contains(","), span:contains("London"), span:contains("Manchester")',
                                    '.location',
                                ]) || null;
                                
                                const salary = pickText($container, [
                                    'span:contains("£"), span:contains("per")',
                                    '.salary',
                                ]) || null;
                                
                                const date_posted = pickText($container, [
                                    'span:contains("ago"), span:contains("hours"), span:contains("days")',
                                    '.posted',
                                ]) || null;

                                jobLinks.push({
                                    url: fullUrl,
                                    userData: {
                                        seed: { title, company, location, salary, date_posted },
                                        referer: request.url,
                                    },
                                });
                            }
                        }
                    });

                    crawlerLog.info(`Found ${jobLinks.length} unique job links on page ${pagesVisited}`);

                    // Enqueue job detail pages
                    if (collectDetails && jobLinks.length > 0) {
                        // Calculate how many more jobs we need
                        const needed = RESULTS_WANTED - saved;
                        // Enqueue extra to account for failures (1.5x buffer)
                        const toEnqueue = jobLinks.slice(0, Math.ceil(needed * 1.5));
                        if (toEnqueue.length > 0) {
                            await enqueueLinks({
                                urls: toEnqueue.map(j => j.url),
                                transformRequestFunction: (req) => {
                                    const match = jobLinks.find(j => j.url === req.url);
                                    if (match) {
                                        req.userData = match.userData;
                                    }
                                    return req;
                                },
                            });
                            crawlerLog.info(`Enqueued ${toEnqueue.length} job detail pages (need ${needed} more jobs)`);
                        }
                    } else if (!collectDetails && jobLinks.length > 0) {
                        // Save minimal data from listing
                        const toPush = jobLinks.slice(0, RESULTS_WANTED - saved).map(j => ({
                            title: j.userData.seed.title,
                            company: j.userData.seed.company,
                            location: j.userData.seed.location,
                            salary: j.userData.seed.salary,
                            date_posted: j.userData.seed.date_posted,
                            job_url: j.url,
                            job_type: null,
                            job_category: null,
                            description_html: null,
                            description_text: null,
                        }));
                        await Dataset.pushData(toPush);
                        saved += toPush.length;
                        crawlerLog.info(`Saved ${toPush.length} jobs (total: ${saved})`);
                    }

                    // Pagination: follow next page link
                    if (pagesVisited < MAX_PAGES) {
                        // Extract current page number from URL
                        const currentUrl = new URL(request.url);
                        const currentPage = parseInt(currentUrl.searchParams.get('page') || '1', 10);
                        const nextPageNum = currentPage + 1;
                        
                        // Build next page URL by setting page parameter
                        currentUrl.searchParams.set('page', nextPageNum.toString());
                        const nextPage = currentUrl.href;
                        
                        if (!seenUrls.has(nextPage)) {
                            seenUrls.add(nextPage);
                            await enqueueLinks({
                                urls: [nextPage],
                                transformRequestFunction: (req) => {
                                    req.userData = { referer: request.url };
                                    return req;
                                },
                            });
                            crawlerLog.info(`Enqueued next page ${nextPageNum}: ${nextPage}`);
                        }
                    }
                }

                // DETAIL PAGE: extract full job details
                if (isDetailPage) {
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.debug('Reached results limit, skipping detail page');
                        return;
                    }

                    const seed = request.userData?.seed || {};
                    
                    // Check if we got valid HTML (not blocked/error page)
                    const htmlText = $.html();
                    const isBlockedOrError = !htmlText || htmlText.length < 500 || 
                        /access denied|blocked|captcha|error/i.test(htmlText.substring(0, 1000));
                    
                    // If blocked, save seed data and continue
                    if (isBlockedOrError && seed.title) {
                        const fallbackRecord = {
                            title: seed.title,
                            company: seed.company,
                            location: seed.location,
                            salary: seed.salary,
                            date_posted: seed.date_posted,
                            job_type: null,
                            job_category: null,
                            description_html: null,
                            description_text: null,
                            job_url: request.loadedUrl || request.url,
                        };
                        await Dataset.pushData(fallbackRecord);
                        saved++;
                        crawlerLog.info(`Saved job #${saved} (from listing data): ${fallbackRecord.title}`);
                        return;
                    }

                    // Try JSON-LD structured data first (best quality)
                    const jsonLd = extractJsonLd($, request.url);

                    // Fallback to HTML parsing
                    const title = jsonLd?.title || 
                        seed.title || 
                        pickText($, ['h1', '.job-title', '[data-automation="job-detail-title"]']) ||
                        null;

                    const company = jsonLd?.company || 
                        seed.company || 
                        pickText($, ['a[href*="/jobs/"]', '.company', '[data-automation="advertiser-name"]']) ||
                        null;

                    const location = jsonLd?.location || 
                        seed.location || 
                        pickText($, ['.location', '[data-automation="job-detail-location"]', 'span:contains(",")']) ||
                        null;

                    const salary = jsonLd?.salary || 
                        seed.salary || 
                        pickText($, ['.salary', '[data-automation="job-detail-salary"]', 'span:contains("£")']) ||
                        null;

                    const date_posted = jsonLd?.date_posted || 
                        seed.date_posted || 
                        pickText($, ['.posted', 'time', 'span:contains("ago")']) ||
                        null;

                    const job_type = jsonLd?.job_type || 
                        pickText($, ['.job-type', '[data-automation="job-detail-worktype"]', 'span:contains("Full-time"), span:contains("Part-time")']) ||
                        null;

                    // Description: prefer JSON-LD, then multiple selectors
                    let description_html = jsonLd?.description_html || '';
                    if (!description_html) {
                        const descNode = $('.job-description').first();
                        if (!descNode.length) {
                            const altNodes = $('[class*="description"], [id*="description"], section, article').filter((i, el) => {
                                const text = $(el).text();
                                return text.length > 200 && /responsibilities|requirements|skills|experience/i.test(text);
                            });
                            if (altNodes.length) {
                                description_html = $(altNodes.first()).html() || '';
                            }
                        } else {
                            description_html = descNode.html() || '';
                        }
                    }

                    const description_text = description_html 
                        ? cheerioLoad(description_html).text().replace(/\s+/g, ' ').trim()
                        : '';

                    // Extract job category from breadcrumbs or meta
                    const job_category = pickText($, ['nav a', '.breadcrumb a', 'meta[name="category"]']) || null;

                    const record = {
                        title,
                        company,
                        location,
                        salary,
                        date_posted,
                        job_type,
                        job_category,
                        description_html,
                        description_text,
                        job_url: request.loadedUrl || request.url,
                    };

                    // Validate: must have at least title and URL
                    if (record.title && record.job_url) {
                        await Dataset.pushData(record);
                        saved++;
                        crawlerLog.info(`Saved job #${saved}: ${record.title}`);
                    } else {
                        crawlerLog.warning(`Skipped incomplete job: ${request.url}`);
                    }
                }
            },

            // Error handling with smart retry
            failedRequestHandler: async ({ request, session }, error) => {
                const message = error?.message || '';
                const statusCode = error?.statusCode || 0;
                const is403or429 = statusCode === 403 || statusCode === 429 || message.includes('403') || message.includes('429') || message.includes('blocked');
                const isTimeout = message.includes('timed out') || message.includes('timeout');
                const isSocketError = message.includes('socket hang up') || message.includes('ECONNRESET') || message.includes('ETIMEDOUT');
                const isHttp2Error = message.includes('NGHTTP2');
                const isNetworkError = isHttp2Error || isSocketError || message.includes('ENOTFOUND');
                
                // Only add to failed set if it's not a recoverable error
                const isRecoverable = is403or429 || isTimeout || isNetworkError;
                if (!isRecoverable) {
                    failedUrls.add(request.url);
                }
                
                if (is403or429) {
                    log.warning(`Blocked (403/429) on ${request.url} - rotating session`);
                    session?.retire();
                    // Longer delay on blocks
                    await randomDelay(blockDelay.min, blockDelay.max);
                } else if (isSocketError) {
                    log.warning(`Socket error on ${request.url} (attempt ${request.retryCount + 1}) - will retry`);
                    session?.markBad();
                    // Short delay for socket issues
                    await randomDelay(800, 1500);
                } else if (isTimeout) {
                    log.warning(`Timeout on ${request.url} (attempt ${request.retryCount + 1}) - will retry`);
                    session?.markBad();
                    await randomDelay(500, 1000);
                } else if (isHttp2Error) {
                    log.debug(`HTTP/2 transport error on ${request.url}, retrying with new session.`);
                    session?.retire();
                    await randomDelay(300, 800);
                } else if (isNetworkError) {
                    log.debug(`Network error on ${request.url}: ${error.message}`);
                    session?.markBad();
                    await randomDelay(400, 900);
                } else {
                    log.error(`Failed ${request.url} after ${request.retryCount + 1} retries: ${error.message}`);
                    // Don't retry non-network errors
                    failedUrls.add(request.url);
                }
            },
        });

        await crawler.run(initial);

        log.info(`✅TotalJobs scraper finished. Saved ${saved} jobs from ${pagesVisited} pages.`);
    } catch (error) {
        log.error(`Fatal error in main: ${error.message}`, { stack: error.stack });
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    log.error(`Unhandled error: ${err.message}`);
    console.error(err);
    process.exit(1);
});
