// TotalJobs scraper - Production-ready implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';

// Selector documentation based on actual Totaljobs.com structure (Nov 2025)
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

const headerGenerator = new HeaderGenerator({
  browsers: [
    {
      name: 'chrome',
      minVersion: 120,
      maxVersion: 122
    }
  ],
  devices: ['desktop'],
  operatingSystems: ['windows'],
});

let warmupCookieHeader = '';

const injectDynamicHeaders = (options) => {
  options.headers = options.headers || {};
  const dynamicHeaders = headerGenerator.getHeaders();
  if (warmupCookieHeader) {
    dynamicHeaders.cookie = warmupCookieHeader;
  }
  options.headers = {
    ...dynamicHeaders,
    ...options.headers,
  };
  options.headers['accept-language'] ??= 'en-GB,en-US;q=0.9,en;q=0.8';
  options.headers['cache-control'] ??= 'no-cache';
  options.headers['pragma'] ??= 'no-cache';
  options.headers['sec-fetch-site'] ??= 'same-origin';
  options.headers['sec-fetch-mode'] ??= 'navigate';
  options.headers['sec-fetch-user'] ??= '?1';
};

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
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;

        // Build start URL from keyword/location or use provided URL
        const buildStartUrl = (kw, loc, cat, posted) => {
            const base = 'https://www.totaljobs.com/jobs';
            if (!kw && !loc && !cat) return `${base}/admin`;
            const u = new URL(base + (kw ? `/${encodeURIComponent(kw)}` : ''));
            if (loc) u.searchParams.set('Location', loc);
            if (cat) u.searchParams.set('Category', cat);
            if (posted && [1, 3, 7].includes(Number(posted))) u.searchParams.set('postedWithin', posted);
            return u.href;
        };

        const initial = [];
        if (startUrl) initial.push(startUrl);
        else if (url) initial.push(url);
        else initial.push(buildStartUrl(keyword, location, category, postedWithin));

        log.info(`TotalJobs scraper started with ${initial.length} start URL(s)`);
        log.info(`Target: ${RESULTS_WANTED} jobs, max ${MAX_PAGES} pages, collectDetails: ${collectDetails}`);

        const startRequests = initial.map((url) => ({
            url,
            userData: { referer: 'https://www.totaljobs.com/' },
            headers: {
                'referer': 'https://www.totaljobs.com/',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            },
        }));

        // Proxy configuration
        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : undefined;

        let saved = 0;
        let pagesVisited = 0;
        const seenUrls = new Set();
        const failedUrls = new Set();

        await Dataset.open('totaljobs-jobs');

        // Determine optimal concurrency based on user input or defaults
        const maxConcurrency = input.maxConcurrency || 8;
        
        // Stealth best practices: optimized speed with maintained stealth
        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 4,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 100,
                sessionOptions: {
                    maxUsageCount: 15,
                    maxErrorScore: 2,
                },
            },
            maxConcurrency: maxConcurrency,
            minConcurrency: Math.max(1, Math.floor(maxConcurrency / 2)),
            requestHandlerTimeoutSecs: 60,
            navigationTimeoutSecs: 45,
            maxRequestsPerMinute: 150,
            ignoreSslErrors: true,
            persistCookiesPerSession: false,
            additionalMimeTypes: ['application/json'],
            preNavigationHooks: [
                async ({ request, session }) => {
                    const retryCount = request.retryCount || 0;
                    const delayBase = retryCount > 0
                        ? getBackoffDelay(retryCount)
                        : Math.floor(Math.random() * 500) + 600;
                    await randomDelay(delayBase, delayBase + 800);
                    if (session && retryCount > 2) {
                        session.markBad();
                    }
                },
            ],

            async requestHandler({ request, $, enqueueLinks, session, log: crawlerLog }) {
                // Inject dynamic headers for this request
                const dynamicHeaders = headerGenerator.getHeaders();
                request.headers = {
                    ...dynamicHeaders,
                    ...request.headers,
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'cache-control': 'no-cache',
                    'pragma': 'no-cache',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-user': '?1',
                };

                const isDetailPage = /\/job\/[^/]+\/[^/]+-job\d+/.test(request.url);
                const isListPage = /\/jobs\//.test(request.url) && !isDetailPage;

                // LIST PAGE: extract job links and pagination
                if (isListPage) {
                    pagesVisited++;
                    crawlerLog.info(`📄 Processing list page ${pagesVisited}/${MAX_PAGES}: ${request.url}`);
                    
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.info(`✅ Reached target of ${RESULTS_WANTED} jobs, stopping pagination`);
                        return;
                    }
                    
                    if (pagesVisited > MAX_PAGES) {
                        crawlerLog.info(`Reached max pages limit (${MAX_PAGES})`);
                        return;
                    }

                    // Optimized delay for list pages - balanced speed and stealth
                    await randomDelay(800, 1500);

                    // Debug pagination elements
                    const nextButtons = $('a:contains("Next")');
                    const pageLinks = $('a[href*="?page="]');
                    const allLinks = $('a[href*="page"]');
                    crawlerLog.info(`Page ${pagesVisited}: Found ${nextButtons.length} Next buttons, ${pageLinks.length} page links, ${allLinks.length} total page-related links`);
                    
                    if (nextButtons.length > 0) {
                        nextButtons.each((i, el) => {
                            const href = $(el).attr('href');
                            crawlerLog.debug(`Next button ${i}: href="${href}", text="${$(el).text().trim()}"`);
                        });
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
                        const toEnqueue = jobLinks.slice(0, RESULTS_WANTED - saved);
                        if (toEnqueue.length > 0) {
                            await enqueueLinks({
                                urls: toEnqueue.map(j => j.url),
                                transformRequestFunction: (req) => {
                                    const match = jobLinks.find(j => j.url === req.url);
                                    if (match) {
                                        req.userData = match.userData;
                                    }
                                    req.headers = req.headers || {};
                                    req.headers.referer = request.url;
                                    return req;
                                },
                            });
                            crawlerLog.info(`Enqueued ${toEnqueue.length} job detail pages`);
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

                    // Pagination: follow next page link - AGGRESSIVE APPROACH
                    if (saved < RESULTS_WANTED && pagesVisited < MAX_PAGES) {
                        const currentUrlObj = new URL(request.url);
                        const currentPage = currentUrlObj.searchParams.has('page')
                            ? Number(currentUrlObj.searchParams.get('page'))
                            : 1;

                        let nextPageUrl = null;

                        // Method 1: Look for "Next" button with href
                        const nextButton = $('a:contains("Next")').first();
                        if (nextButton.length && nextButton.attr('href')) {
                            const nextHref = nextButton.attr('href');
                            nextPageUrl = nextHref.startsWith('http') 
                                ? nextHref 
                                : `https://www.totaljobs.com${nextHref.startsWith('/') ? nextHref : '/' + nextHref}`;
                            crawlerLog.info(`Found Next button: ${nextPageUrl}`);
                        }

                        // Method 2: Look for numbered page links and find the next one
                        if (!nextPageUrl) {
                            const pageLinks = $('a[href*="page="]');
                            pageLinks.each((i, el) => {
                                const href = $(el).attr('href');
                                if (href) {
                                    const match = href.match(/page=(\d+)/);
                                    if (match) {
                                        const pageNum = Number(match[1]);
                                        if (pageNum === currentPage + 1) {
                                            nextPageUrl = href.startsWith('http') 
                                                ? href 
                                                : `https://www.totaljobs.com${href.startsWith('/') ? href : '/' + href}`;
                                            return false; // break
                                        }
                                    }
                                }
                            });
                            if (nextPageUrl) {
                                crawlerLog.info(`Found page link for page ${currentPage + 1}: ${nextPageUrl}`);
                            }
                        }

                        // Method 3: If no Next button or page link, construct next page URL manually
                        if (!nextPageUrl) {
                            const nextPageNum = currentPage + 1;
                            const nextUrlObj = new URL(request.url);
                            nextUrlObj.searchParams.set('page', nextPageNum.toString());
                            nextPageUrl = nextUrlObj.href;
                            crawlerLog.info(`🔧 Constructed next page URL: ${nextPageUrl}`);
                        }

                        // Always try to enqueue next page if we haven't reached limits
                        if (nextPageUrl) {
                            const nextUrlObj = new URL(nextPageUrl);
                            const nextPageNum = nextUrlObj.searchParams.has('page') 
                                ? Number(nextUrlObj.searchParams.get('page')) 
                                : 2;
                            
                            // More lenient validation - trust the URL construction
                            if (nextPageNum > currentPage && nextPageNum <= MAX_PAGES) {
                                if (!seenUrls.has(nextPageUrl)) {
                                    seenUrls.add(nextPageUrl);
                                    await enqueueLinks({
                                        urls: [nextPageUrl],
                                        transformRequestFunction: (req) => {
                                            req.userData = { referer: request.url, isListPage: true };
                                            req.headers = req.headers || {};
                                            req.headers.referer = request.url;
                                            // Higher priority for pagination
                                            req.retryCount = 0;
                                            return req;
                                        },
                                    });
                                    crawlerLog.info(`✅ Enqueued next page ${nextPageNum}/${MAX_PAGES} (${saved}/${RESULTS_WANTED} jobs saved)`);
                                } else {
                                    crawlerLog.debug(`⏭️ Page already queued: ${nextPageUrl}`);
                                }
                            } else {
                                crawlerLog.info(`🛑 Page ${nextPageNum} exceeds limits (max: ${MAX_PAGES})`);
                            }
                        } else {
                            crawlerLog.warning('⚠️ Could not determine next page URL');
                        }
                    } else if (saved >= RESULTS_WANTED) {
                        crawlerLog.info(`🎯 Target reached (${saved}/${RESULTS_WANTED} jobs), stopping pagination`);
                    }
                }

                // DETAIL PAGE: extract full job details
                if (isDetailPage) {
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.debug('Reached results limit, skipping detail page');
                        return;
                    }

                    // Optimized delay for detail pages - balanced speed and stealth
                    await randomDelay(1000, 2000);

                    const seed = request.userData?.seed || {};

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
                        crawlerLog.info(`✓ Saved job #${saved}: ${record.title}`);
                    } else {
                        crawlerLog.warning(`Skipped incomplete job: ${request.url}`);
                    }
                }
            },

            // Error handling with smart retry - DON'T STOP CRAWLING
            failedRequestHandler: async ({ request, session }, error) => {
                const is403or429 = error.message.includes('403') || error.message.includes('429');
                const isNetworkError = error.message.includes('NGHTTP2') || error.message.includes('socket') || 
                                      error.message.includes('ECONNRESET') || error.message.includes('ETIMEDOUT') ||
                                      error.message.includes('ENOTFOUND') || error.message.includes('EAI_AGAIN');
                const isListPage = request.userData?.isListPage || /\/jobs\//.test(request.url);
                
                if (is403or429) {
                    log.warning(`🚫 Blocked (${error.message.includes('403') ? '403' : '429'}) on ${request.url} - retry ${request.retryCount}/4`);
                    // Mark session as bad to force rotation
                    if (session) {
                        session.markBad();
                    }
                    // Optimized backoff for blocking
                    await randomDelay(3000, 5000);
                    
                    // For list pages, try to re-enqueue with fresh session
                    if (isListPage && request.retryCount < 2) {
                        log.info(`🔄 Re-enqueueing list page with fresh session`);
                        // Will be retried automatically by crawler
                    }
                } else if (isNetworkError) {
                    log.warning(`🌐 Network error on ${request.url}: ${error.message.substring(0, 100)}`);
                    // Mark session as bad for connection errors
                    if (session) {
                        session.markBad();
                    }
                    // Optimized backoff for network issues
                    await randomDelay(2000, 4000);
                } else {
                    log.error(`❌ Failed ${request.url} after ${request.retryCount} retries: ${error.message.substring(0, 150)}`);
                }
                
                // Mark as failed but DON'T add to failedUrls if it's a list page - we want to keep trying
                if (!isListPage) {
                    failedUrls.add(request.url);
                }
                
                log.info(`📊 Progress: ${saved}/${RESULTS_WANTED} jobs saved, ${pagesVisited}/${MAX_PAGES} pages visited`);
            },
        });

        await crawler.run(startRequests);

        // Final stats for QA and monitoring
        const stats = {
            jobsSaved: saved,
            pagesVisited: pagesVisited,
            targetJobs: RESULTS_WANTED,
            maxPages: MAX_PAGES,
            uniqueUrlsSeen: seenUrls.size,
            failedUrls: failedUrls.size,
        };
        
        log.info(`✅ TotalJobs scraper finished successfully!`);
        log.info(`📊 Final Stats: ${JSON.stringify(stats, null, 2)}`);
        
        // Set output for Apify platform
        await Actor.setValue('OUTPUT', stats);
        
    } catch (error) {
        log.error(`Fatal error in main: ${error.message}`, { stack: error.stack });
        await Actor.setValue('OUTPUT', { 
            error: error.message, 
            jobsSaved: 0,
            status: 'FAILED'
        });
        throw error;
    }
}

main()
    .then(() => {
        log.info('Actor completed successfully');
    })
    .catch(async (err) => {
        log.error(`Unhandled error: ${err.message}`);
        console.error(err);
        await Actor.setValue('OUTPUT', { 
            error: err.message, 
            stack: err.stack,
            status: 'FAILED'
        });
    })
    .finally(async () => {
        await Actor.exit();
    });
