//TotalJobs scraper - Production-ready implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

const REFERER_FALLBACK = 'https://www.google.com/';
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36';
const DEFAULT_SEC_CH_UA = '"Chromium";v="118", "Google Chrome";v="118", "Not;A=Brand";v="99"';

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

        const maxRequestsPerMinute = Number.isFinite(+inputMaxRpm)
            ? Math.min(160, Math.max(50, +inputMaxRpm))
            : 110;
        const maxConcurrency = Number.isFinite(+inputMaxConcurrency)
            ? Math.max(4, Math.min(16, +inputMaxConcurrency))
            : 8;
        const minConcurrency = Number.isFinite(+inputMinConcurrency)
            ? Math.max(2, Math.min(maxConcurrency, +inputMinConcurrency))
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
            log.info('Proxy configuration enabled for anti-blocking');
        } else {
            log.warning('Running without proxies - may face rate limiting. Consider enabling Apify proxies for better results.');
        }

        let saved = 0;
        let pagesVisited = 0;
        const seenUrls = new Set();
        const failedUrls = new Set();
        const savedJobUrls = new Set();

        await Dataset.open('totaljobs-jobs');

        const requestQueue = await Actor.openRequestQueue();

        const saveJobRecord = async (record, logger, label) => {
            if (!record?.job_url || !record?.title) return false;
            if (saved >= RESULTS_WANTED) return false;
            if (savedJobUrls.has(record.job_url)) return false;
            await Dataset.pushData(record);
            savedJobUrls.add(record.job_url);
            saved++;
            if (logger && label) {
                logger.info(`${label} ${record.title} (total: ${saved})`);
            }
            return true;
        };

        // Stealth best practices: balanced speed and stealth
        const crawler = new CheerioCrawler({
            requestQueue,
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
                    const referer = request.userData?.referer || REFERER_FALLBACK;
                    const userAgent = request.userData?.userAgent || DEFAULT_USER_AGENT;
                    const host = new URL(request.url).host;
                    
                    if (!gotoOptions.headers) gotoOptions.headers = {};
                    Object.assign(gotoOptions.headers, {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Cache-Control': 'no-cache',
                        'DNT': '1',
                        'Host': host,
                        'Connection': 'keep-alive',
                        'Referer': referer,
                        'User-Agent': userAgent,
                        'Sec-CH-UA': DEFAULT_SEC_CH_UA,
                        'Sec-CH-UA-Mobile': '?0',
                        'Sec-CH-UA-Platform': '"Windows"',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': referer.includes('totaljobs') ? 'same-origin' : 'cross-site',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                    });

                    // Force HTTP/1.1 because TotalJobs intermittently closes HTTP/2 streams
                    gotoOptions.http2 = false;
                    gotoOptions.timeout = Math.max(gotoOptions.timeout ?? 0, 60000);
                    
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
                        const needed = Math.max(0, RESULTS_WANTED - saved);
                        const bufferSize = Math.max(1, Math.ceil(needed * 1.5));
                        const toEnqueue = jobLinks.slice(0, Math.min(bufferSize, jobLinks.length));
                        if (toEnqueue.length > 0) {
                            await enqueueLinks({
                                requests: toEnqueue.map((job) => ({
                                    url: job.url,
                                    userData: {
                                        ...job.userData,
                                        referer: request.url,
                                        requeueAttempt: job.userData?.requeueAttempt ?? 0,
                                    },
                                })),
                            });
                            crawlerLog.info(`Enqueued ${toEnqueue.length} job detail pages (need ${Math.max(1, needed)} more jobs)`);
                        }
                    } else if (!collectDetails && jobLinks.length > 0) {
                        for (const job of jobLinks) {
                            if (saved >= RESULTS_WANTED) break;
                            const seed = job.userData.seed || {};
                            await saveJobRecord({
                                title: seed.title,
                                company: seed.company,
                                location: seed.location,
                                salary: seed.salary,
                                date_posted: seed.date_posted,
                                job_type: null,
                                job_category: null,
                                description_html: null,
                                description_text: null,
                                job_url: job.url,
                            }, crawlerLog, 'Saved job from listing:');
                        }
                    }

                    await randomDelay(listDelay.min, listDelay.max);

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
                    await randomDelay(detailDelay.min, detailDelay.max);
                    
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
                        const savedFallback = await saveJobRecord(fallbackRecord, crawlerLog, 'Saved job from listing data:');
                        if (savedFallback) {
                            failedUrls.add(request.url);
                        }
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
                    if (!record.title || !record.job_url) {
                        crawlerLog.warning(`Skipped incomplete job: ${request.url}`);
                        failedUrls.add(request.url);
                        return;
                    }

                    const savedDetail = await saveJobRecord(record, crawlerLog, 'Saved job:');
                    if (!savedDetail) {
                        crawlerLog.debug(`Skipped duplicate or limit reached for ${request.url}`);
                    }
                }
            },

            // Error handling with smart retry
            failedRequestHandler: async ({ request, session }, error) => {
                const url = request.url;
                const message = error?.message || '';
                const statusCode = error?.statusCode || 0;
                const attempt = request.retryCount + 1;
                const isDetailPage = /\/job\/[^/]+\/[^/]+-job\d+/.test(url);
                const isListPage = /\/jobs\//.test(url) && !isDetailPage;

                const is403or429 = statusCode === 403 || statusCode === 429 || /403|429|blocked/i.test(message);
                const isTimeout = /timed out|timeout/i.test(message);
                const isSocketError = /socket hang up|ECONNRESET|ETIMEDOUT/i.test(message);
                const isHttp2Error = /NGHTTP2/i.test(message);
                const isNetworkError = isHttp2Error || isSocketError || /ENOTFOUND|EAI_AGAIN/i.test(message);

                if (isDetailPage && collectDetails && request.userData?.seed && !failedUrls.has(url)) {
                    const seed = request.userData.seed;
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
                        job_url: request.loadedUrl || url,
                    };
                    const savedFallback = await saveJobRecord(fallbackRecord, log, 'Saved job after failed detail:');
                    if (savedFallback) {
                        failedUrls.add(url);
                    }
                }

                if (isListPage) {
                    const requeueAttempt = Number(request.userData?.requeueAttempt || 0);
                    if (requeueAttempt < 2 && saved < RESULTS_WANTED) {
                        await requestQueue.addRequest({
                            url,
                            uniqueKey: `${url}#retry-${Date.now()}`,
                            userData: {
                                ...request.userData,
                                referer: request.userData?.referer || REFERER_FALLBACK,
                                requeueAttempt: requeueAttempt + 1,
                            },
                        });
                        log.warning(`Requeued listing page after failure (attempt ${requeueAttempt + 1}): ${url}`);
                    } else {
                        failedUrls.add(url);
                    }
                } else if (!isNetworkError && !isTimeout && !is403or429) {
                    failedUrls.add(url);
                }

                if (is403or429) {
                    log.warning(`Blocked (403/429) on ${url} - rotating session`);
                    session?.retire();
                    await randomDelay(blockDelay.min, blockDelay.max);
                } else if (isSocketError) {
                    log.warning(`Socket error on ${url} (attempt ${attempt}) - will retry`);
                    session?.markBad();
                    await randomDelay(800, 1500);
                } else if (isTimeout) {
                    log.warning(`Timeout on ${url} (attempt ${attempt}) - will retry`);
                    session?.markBad();
                    await randomDelay(500, 1200);
                } else if (isHttp2Error) {
                    log.debug(`HTTP/2 transport error on ${url}, retrying with new session.`);
                    session?.retire();
                    await randomDelay(300, 800);
                } else if (isNetworkError) {
                    log.debug(`Network error on ${url}: ${message}`);
                    session?.markBad();
                    await randomDelay(400, 900);
                } else {
                    log.error(`Failed ${url} after ${attempt} retries: ${message}`);
                    failedUrls.add(url);
                }
            },
        });

        await crawler.run(initial);

        log.info(`TotalJobs scraper finished. Saved ${saved} jobs from ${pagesVisited} pages.`);
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












