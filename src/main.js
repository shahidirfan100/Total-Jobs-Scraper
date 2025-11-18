// TotalJobs scraper - Production-ready implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';
import { gotScraping } from 'got-scraping';

const JOB_PATH_REGEX = /\/job\/[^/?#]+\/[^/?#]+-job\d+/i;
const JOB_DETAIL_REGEX = /^https?:\/\/(?:www\.)?totaljobs\.com\/job\/[^/?#]+\/[^/?#]+-job\d+/i;
const LIST_PATH_REGEX = /\/jobs\//i;

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

function extractResultListState($) {
  const scripts = $('script');
  for (let i = 0; i < scripts.length; i++) {
    const content = $(scripts[i]).html();
    if (!content) continue;
    const match = content.match(/window\.__PRELOADED_STATE__\["app-unifiedResultlist"\]\s*=\s*(\{[\s\S]*?\});/);
    if (match) {
      try {
        return JSON.parse(match[1]);
      } catch (err) {
        log.debug(`Result list state parse error: ${err.message}`);
      }
    }
  }
  return null;
}

function htmlToText(html) {
  if (!html) return '';
  return cheerioLoad(`<body>${html}</body>`).text().replace(/\s+/g, ' ').trim();
}

function buildPageUrl(currentUrl, targetPage) {
  const urlObj = new URL(currentUrl);
  urlObj.searchParams.delete('of');
  urlObj.searchParams.delete('Of');
  urlObj.searchParams.delete('action');
  if (targetPage <= 1) {
    urlObj.searchParams.delete('page');
  } else {
    urlObj.searchParams.set('page', targetPage.toString());
  }
  return urlObj.href;
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
  
  // Preserve existing referer if set
  const existingReferer = options.headers.referer || options.headers.Referer;
  
  if (warmupCookieHeader) {
    dynamicHeaders.cookie = warmupCookieHeader;
  }
  
  options.headers = {
    ...dynamicHeaders,
    ...options.headers,
  };
  
  // Restore referer if it was overwritten
  if (existingReferer) {
    options.headers['referer'] = existingReferer;
  }
  
  // Enhanced stealth headers
  options.headers['accept'] ??= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8';
  options.headers['accept-language'] ??= 'en-GB,en-US;q=0.9,en;q=0.8';
  options.headers['accept-encoding'] ??= 'gzip, deflate, br';
  options.headers['cache-control'] ??= 'max-age=0';
  options.headers['sec-ch-ua'] ??= '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"';
  options.headers['sec-ch-ua-mobile'] ??= '?0';
  options.headers['sec-ch-ua-platform'] ??= '"Windows"';
  options.headers['sec-fetch-dest'] ??= 'document';
  options.headers['sec-fetch-mode'] ??= 'navigate';
  options.headers['sec-fetch-site'] ??= existingReferer ? 'same-origin' : 'none';
  options.headers['sec-fetch-user'] ??= '?1';
  options.headers['upgrade-insecure-requests'] ??= '1';
  
  // Remove headers that might expose automation
  delete options.headers['x-crawlee'];
  delete options.headers['X-Crawlee'];
};

async function warmUpSite(proxyConf) {
  try {
    const proxyUrl = proxyConf ? await proxyConf.newUrl() : undefined;
    const response = await gotScraping({
      url: 'https://www.totaljobs.com/',
      proxyUrl,
      timeout: { request: 15000 },
      retry: { limit: 1 },
      headers: headerGenerator.getHeaders(),
    });
    const setCookie = response.headers['set-cookie'];
    if (setCookie) {
      warmupCookieHeader = Array.isArray(setCookie)
        ? setCookie.map((cookie) => cookie.split(';')[0]).join('; ')
        : setCookie.split(';')[0];
      log.info('Warm-up request succeeded, cookies captured');
    }
  } catch (err) {
    log.warning(`Warm-up request failed: ${err.message}`);
  }
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

        const startRequests = initial.map((currentUrl) => {
            const request = {
                url: currentUrl,
                userData: { referer: 'https://www.totaljobs.com/', isListPage: true, pageNum: 1 },
                headers: {
                    referer: 'https://www.totaljobs.com/',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                },
            };
            injectDynamicHeaders(request);
            return request;
        });

        // Proxy configuration
        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : undefined;

        await warmUpSite(proxyConf);

        const requestQueue = await Actor.openRequestQueue();
        await requestQueue.addRequests(startRequests);

        let saved = 0;
        let pagesVisited = 0;
        const seenJobUrls = new Set();
        const seenPageUrls = new Set(startRequests.map((req) => req.url));
        const failedUrls = new Set();
        let shouldAbort = false;

        // Determine optimal concurrency based on user input or defaults
        const maxConcurrency = Number.isFinite(+input.maxConcurrency)
            ? Math.max(1, Math.min(12, +input.maxConcurrency))
            : 8;
        
        // Stealth best practices: optimized speed with maintained stealth
        const crawler = new CheerioCrawler({
            requestQueue,
            proxyConfiguration: proxyConf,
            maxRequestRetries: 4,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 100,
                sessionOptions: {
                    maxUsageCount: 20,
                    maxErrorScore: 3,
                },
            },
            maxConcurrency: maxConcurrency,
            minConcurrency: Math.max(1, Math.floor(maxConcurrency / 2)),
            requestHandlerTimeoutSecs: 60,
            navigationTimeoutSecs: 45,
            maxRequestsPerMinute: 220,
            ignoreSslErrors: true,
            persistCookiesPerSession: true,
            additionalMimeTypes: ['application/json'],
            preNavigationHooks: [
                async ({ request, session, crawler: crawlerInstance }) => {
                    // Abort if target reached
                    if (shouldAbort || saved >= RESULTS_WANTED) {
                        log.info(`Aborting request - target reached: ${request.url}`);
                        shouldAbort = true;
                        return;
                    }
                    
                    // Inject fresh dynamic headers per request
                    injectDynamicHeaders(request);
                    
                    const retryCount = request.retryCount || 0;
                    const isListPage = request.userData?.isListPage;
                    
                    // More aggressive delays for list pages (anti-bot detection)
                    const delayBase = retryCount > 0
                        ? getBackoffDelay(retryCount)
                        : isListPage 
                            ? Math.floor(Math.random() * 800) + 1200  // 1200-2000ms for list pages
                            : Math.floor(Math.random() * 400) + 700;  // 700-1100ms for detail pages
                    
                    await randomDelay(delayBase, delayBase + 600);
                    
                    if (session && retryCount > 2) {
                        session.markBad();
                    }
                },
            ],
            // Force HTTP/1.1 to avoid HTTP/2 NGHTTP2_INTERNAL_ERROR
            requestHandlerOptions: {
                http2: false,
            },

            async requestHandler({ request, $, session, log: crawlerLog, crawler: crawlerInstance }) {
                // Early abort check
                if (shouldAbort || saved >= RESULTS_WANTED) {
                    crawlerLog.info(`Skipping request - target reached (${saved}/${RESULTS_WANTED})`);
                    return;
                }

                const loadedUrl = request.loadedUrl || request.url;
                const urlObj = new URL(loadedUrl);
                const isDetailPage = request.userData?.isDetailPage
                    || JOB_DETAIL_REGEX.test(loadedUrl)
                    || JOB_PATH_REGEX.test(urlObj.pathname);
                const isListPage = request.userData?.isListPage
                    || (LIST_PATH_REGEX.test(urlObj.pathname) && !isDetailPage);


// LIST PAGE: extract job links and pagination
if (isListPage) {
    pagesVisited++;
    crawlerLog.info(`📃 Processing list page ${pagesVisited}/${MAX_PAGES}: ${request.url}`);

    if (saved >= RESULTS_WANTED) {
        crawlerLog.info(`✅ Reached target of ${RESULTS_WANTED} jobs, stopping crawler`);
        shouldAbort = true;
        await crawlerInstance.autoscaledPool?.abort();
        return;
    }

    if (pagesVisited > MAX_PAGES) {
        crawlerLog.info(`Reached max pages limit (${MAX_PAGES})`);
        return;
    }

                    const state = extractResultListState($);
                    const pagination = state?.searchResults?.pagination;
                    const perPage = pagination?.perPage || 25;
                    const derivedFromUrl = (() => {
                        if (urlObj.searchParams.has('page')) {
                            const val = Number(urlObj.searchParams.get('page'));
                            return Number.isFinite(val) ? val : null;
                        }
                        if (urlObj.searchParams.has('of')) {
                            const ofVal = Number(urlObj.searchParams.get('of'));
                            if (Number.isFinite(ofVal)) {
                                return Math.floor(ofVal / perPage) + 1;
                            }
                        }
                        return null;
                    })();
                    let currentPage = request.userData?.pageNum
                        || pagination?.page
                        || derivedFromUrl
                        || 1;

                    let jobLinks = [];
                    let nextPageUrl = null;
                    let nextPageNum = null;

    if (state?.searchResults?.items?.length) {
        const items = state.searchResults.items;
        for (const item of items) {
            const href = item.url;
            if (!href) continue;
            const fullUrl = href.startsWith('http')
                ? href
                : `https://www.totaljobs.com${href.startsWith('/') ? href : `/${href}`}`;
            if (seenJobUrls.has(fullUrl) || failedUrls.has(fullUrl)) continue;
            seenJobUrls.add(fullUrl);

            const snippetHtml = item.textSnippet || '';
            jobLinks.push({
                url: fullUrl,
                userData: {
                    seed: {
                        title: item.title || null,
                        company: item.companyName || null,
                        location: item.location || null,
                        salary: item.salary || null,
                        date_posted: item.datePosted || null,
                        description_html: snippetHtml || null,
                        description_text: htmlToText(snippetHtml) || null,
                        job_id: item.id ? String(item.id) : null,
                    },
                    jobId: item.id,
                },
            });
        }
                        nextPageUrl = state.searchResults.pagination?.links?.next || null;
                        crawlerLog.debug(`State payload provided ${jobLinks.length} jobs`);
                        if (pagination?.pageCount && pagination.page < pagination.pageCount) {
                            nextPageNum = pagination.page + 1;
                            nextPageUrl = buildPageUrl(request.url, nextPageNum);
                        }
    }

    if (!jobLinks.length) {
        const fallbackLinks = [];
        $('a[href^="/job/"]').each((i, el) => {
            const href = $(el).attr('href');
            if (href && href.match(/\/job\/[^/]+\/[^/]+-job\d+/)) {
                const fullUrl = `https://www.totaljobs.com${href}`;
                if (!seenJobUrls.has(fullUrl) && !failedUrls.has(fullUrl)) {
                    seenJobUrls.add(fullUrl);

                    const $link = $(el);
                    const title = $link.text().trim() || null;
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

                    fallbackLinks.push({
                        url: fullUrl,
                        userData: {
                            seed: { title, company, location, salary, date_posted },
                        },
                    });
                }
            }
        });
        jobLinks = fallbackLinks;
        crawlerLog.info(`Fallback DOM extraction found ${jobLinks.length} jobs`);
    } else {
        crawlerLog.info(`Found ${jobLinks.length} jobs via embedded state`);
    }

    if (collectDetails && jobLinks.length > 0) {
        // Only enqueue if we haven't reached target
        const remaining = Math.max(0, RESULTS_WANTED - saved);
        const toEnqueue = jobLinks.slice(0, remaining);
        
        if (toEnqueue.length > 0 && !shouldAbort) {
            const prepared = toEnqueue.map((job) => ({
                url: job.url,
                uniqueKey: job.url,
                userData: {
                    ...job.userData,
                    isDetailPage: true,
                },
                headers: {
                    referer: request.url,
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                },
            }));
            
            // Inject dynamic headers for each request
            prepared.forEach(req => injectDynamicHeaders(req));
            
            await requestQueue.addRequests(prepared);
            crawlerLog.info(`✓ Enqueued ${prepared.length} job detail pages (${saved}/${RESULTS_WANTED} saved)`);
        } else if (remaining <= 0) {
            crawlerLog.info(`Target reached, not enqueueing more detail pages`);
            shouldAbort = true;
        }
    } else if (!collectDetails && jobLinks.length > 0) {
        const remaining = Math.max(0, RESULTS_WANTED - saved);
        const toPush = jobLinks.slice(0, remaining).map(j => ({
            title: j.userData.seed.title,
            company: j.userData.seed.company,
            location: j.userData.seed.location,
            salary: j.userData.seed.salary,
            date_posted: j.userData.seed.date_posted,
            job_url: j.url,
            job_type: null,
            job_category: null,
            description_html: j.userData.seed.description_html || null,
            description_text: j.userData.seed.description_text || null,
        }));
        
        if (toPush.length > 0) {
            await Dataset.pushData(toPush);
            saved += toPush.length;
            crawlerLog.info(`📄 Saved ${toPush.length} jobs (total: ${saved}/${RESULTS_WANTED})`);
            
            if (saved >= RESULTS_WANTED) {
                shouldAbort = true;
                crawlerLog.info(`✅ Target reached, stopping crawler`);
            }
        }
    }

                if (saved < RESULTS_WANTED && pagesVisited < MAX_PAGES && !shouldAbort) {
                    if (!nextPageUrl) {
                        const nextButton = $('a:contains("Next")').first();
                        if (nextButton.length && nextButton.attr('href')) {
                            const nextHref = nextButton.attr('href');
                            nextPageUrl = nextHref.startsWith('http') 
                                ? nextHref 
                                : `https://www.totaljobs.com${nextHref.startsWith('/') ? nextHref : '/' + nextHref}`;
                            nextPageNum = currentPage + 1;
                            crawlerLog.info(`Found Next button: ${nextPageUrl}`);
                        }

                        if (!nextPageUrl) {
                            const pageLinks = $('a[href*="page="]');
                            pageLinks.each((i, el) => {
                                const href = $(el).attr('href');
                                if (href) {
                                    const match = href.match(/page=(\d+)/);
                                    if (match) {
                                        const pageNumCandidate = Number(match[1]);
                                        if (pageNumCandidate === currentPage + 1) {
                                            nextPageUrl = href.startsWith('http') 
                                                ? href 
                                                : `https://www.totaljobs.com${href.startsWith('/') ? href : '/' + href}`;
                                            nextPageNum = pageNumCandidate;
                                            return false;
                                        }
                                    }
                                }
                            });
                        }

                        if (!nextPageUrl) {
                            const manualNext = currentPage + 1;
                            nextPageUrl = buildPageUrl(request.url, manualNext);
                            nextPageNum = manualNext;
                            crawlerLog.info(`🔧 Constructed next page URL: ${nextPageUrl}`);
                        }
                    }

                    if (nextPageUrl && !shouldAbort) {
                        const normalizedNext = nextPageUrl.startsWith('http')
                            ? nextPageUrl
                            : `https://www.totaljobs.com${nextPageUrl.startsWith('/') ? nextPageUrl : `/${nextPageUrl}`}`;
                        const derivedNextNum = normalizedNext.includes('page=')
                            ? Number(new URL(normalizedNext).searchParams.get('page'))
                            : (nextPageNum ?? (currentPage + 1));
                        const safePageNum = Number.isFinite(derivedNextNum) ? derivedNextNum : currentPage + 1;

                        if (safePageNum > currentPage && safePageNum <= MAX_PAGES) {
                            if (!seenPageUrls.has(normalizedNext)) {
                                seenPageUrls.add(normalizedNext);
                                
                                const nextPageReq = {
                                    url: normalizedNext,
                                    uniqueKey: normalizedNext,
                                    userData: { 
                                        referer: request.url, 
                                        isListPage: true, 
                                        pageNum: safePageNum 
                                    },
                                    headers: { 
                                        referer: request.url,
                                        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                                    },
                                };
                                
                                // Inject dynamic headers for pagination request
                                injectDynamicHeaders(nextPageReq);
                                
                                await requestQueue.addRequest(nextPageReq);
                                crawlerLog.info(`➡️ Enqueued next page ${safePageNum}/${MAX_PAGES} (${saved}/${RESULTS_WANTED} jobs saved)`);
                            } else {
                                crawlerLog.debug(`⚠️ Page already queued: ${normalizedNext}`);
                            }
                        } else {
                            crawlerLog.info(`⚠️ Page ${safePageNum} exceeds limits (max: ${MAX_PAGES})`);
                        }
                    } else if (shouldAbort) {
                        crawlerLog.info(`✅ Target reached, not enqueueing more pages`);
                    } else {
                        crawlerLog.warning('⚠️ Could not determine next page URL');
                    }
                } else if (saved >= RESULTS_WANTED) {
                    crawlerLog.info(`✅ Target reached (${saved}/${RESULTS_WANTED} jobs), stopping pagination`);
                    shouldAbort = true;
                }
            }

            // DETAIL PAGE: extract full job details
                if (isDetailPage) {
                    if (saved >= RESULTS_WANTED || shouldAbort) {
                        crawlerLog.debug('Reached results limit, skipping detail page');
                        return;
                    }

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
                    if (record.title && record.job_url && saved < RESULTS_WANTED) {
                        await Dataset.pushData(record);
                        saved++;
                        crawlerLog.info(`✓ Saved job #${saved}/${RESULTS_WANTED}: ${record.title}`);
                        
                        // Check if we reached target
                        if (saved >= RESULTS_WANTED) {
                            shouldAbort = true;
                            crawlerLog.info(`✅ Target of ${RESULTS_WANTED} jobs reached, signaling abort`);
                            await crawlerInstance.autoscaledPool?.abort();
                        }
                    } else if (!record.title || !record.job_url) {
                        crawlerLog.warning(`Skipped incomplete job: ${request.url}`);
                    } else {
                        crawlerLog.info(`Target already reached, skipping save`);
                    }
                }
            },

            // Error handling with smart retry - DON'T STOP CRAWLING
            failedRequestHandler: async ({ request, session, error, log: crawlerLog }) => {
                // Don't process failures if target already reached
                if (shouldAbort || saved >= RESULTS_WANTED) {
                    crawlerLog.info(`Target reached, skipping failed request handling`);
                    return;
                }
                
                const message = error?.message || 'Unknown error';
                const is403or429 = message.includes('403') || message.includes('429');
                const isNetworkError = message.includes('NGHTTP2') || message.includes('socket') ||
                                      message.includes('ECONNRESET') || message.includes('ETIMEDOUT') ||
                                      message.includes('ENOTFOUND') || message.includes('EAI_AGAIN') ||
                                      message.includes('Stream closed');
                const isListPage = request.userData?.isListPage || LIST_PATH_REGEX.test(new URL(request.url).pathname);

                if (is403or429) {
                    crawlerLog.warning(`🚫 Blocked (${message.includes('403') ? '403' : '429'}) on ${request.url} - retry ${request.retryCount}/4`);
                    if (session) {
                        session.retire();
                    }
                    await randomDelay(4000, 7000);

                    // For list pages, try to recover with fresh session and longer delay
                    if (isListPage && request.retryCount >= 3 && !shouldAbort) {
                        const pageNum = request.userData?.pageNum || 1;
                        const fallbackUrl = buildPageUrl(request.url, pageNum);
                        const uniqueRetryKey = `${fallbackUrl}#recovery-${Date.now()}`;
                        
                        if (!seenPageUrls.has(uniqueRetryKey)) {
                            seenPageUrls.add(uniqueRetryKey);
                            const recoveryReq = {
                                url: fallbackUrl,
                                uniqueKey: uniqueRetryKey,
                                userData: {
                                    referer: 'https://www.totaljobs.com/',
                                    isListPage: true,
                                    pageNum: pageNum,
                                },
                                headers: {
                                    referer: 'https://www.totaljobs.com/',
                                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                                },
                            };
                            injectDynamicHeaders(recoveryReq);
                            await requestQueue.addRequest(recoveryReq);
                            crawlerLog.info(`🔄 Recovery: Re-enqueued list page ${pageNum} with fresh session`);
                        }
                    }
                } else if (isNetworkError) {
                    crawlerLog.warning(`🌐 Network error on ${request.url}: ${message.substring(0, 100)}`);
                    if (session) {
                        session.retire();
                    }
                    await randomDelay(3000, 5000);
                    
                    // Retry list pages with manual URL construction
                    if (isListPage && !shouldAbort) {
                        const fallbackPageNum = request.userData?.pageNum
                            || (request.url.includes('page=')
                                ? Number(new URL(request.url).searchParams.get('page'))
                                : null)
                            || 1;
                        const manualUrl = buildPageUrl(request.url, fallbackPageNum);
                        const retryKey = `${manualUrl}#network-retry-${request.retryCount}`;
                        
                        if (!seenPageUrls.has(retryKey) && request.retryCount >= 3) {
                            seenPageUrls.add(retryKey);
                            const retryReq = {
                                url: manualUrl,
                                uniqueKey: retryKey,
                                userData: {
                                    referer: request.userData?.referer || 'https://www.totaljobs.com/',
                                    isListPage: true,
                                    pageNum: fallbackPageNum,
                                },
                                headers: { 
                                    referer: request.userData?.referer || 'https://www.totaljobs.com/',
                                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                                },
                            };
                            injectDynamicHeaders(retryReq);
                            await requestQueue.addRequest(retryReq);
                            crawlerLog.info(`🔄 Re-enqueued list page ${fallbackPageNum} after network error`);
                        }
                    }
                } else {
                    crawlerLog.error(`❌ Failed ${request.url} after ${request.retryCount} retries: ${message.substring(0, 150)}`);
                }

                // Save seed data as fallback for failed detail pages
                if (!isListPage && !shouldAbort) {
                    failedUrls.add(request.url);
                    if (request.userData?.seed && saved < RESULTS_WANTED) {
                        const seed = request.userData.seed;
                        const fallbackRecord = {
                            title: seed.title || null,
                            company: seed.company || null,
                            location: seed.location || null,
                            salary: seed.salary || null,
                            date_posted: seed.date_posted || null,
                            job_type: null,
                            job_category: null,
                            description_html: seed.description_html || null,
                            description_text: seed.description_text || null,
                            job_url: request.loadedUrl || request.url,
                        };
                        if (fallbackRecord.title && fallbackRecord.job_url) {
                            await Dataset.pushData(fallbackRecord);
                            saved++;
                            crawlerLog.info(`📄 Saved fallback seed #${saved}: ${fallbackRecord.title}`);
                            
                            if (saved >= RESULTS_WANTED) {
                                shouldAbort = true;
                                crawlerLog.info(`✅ Target reached via fallback data`);
                            }
                        }
                    }
                }
                
                crawlerLog.info(`📊 Progress: ${saved}/${RESULTS_WANTED} jobs saved, ${pagesVisited}/${MAX_PAGES} pages visited`);
            },
        });

        await crawler.run();

        // Final stats for QA and monitoring
        const stats = {
            jobsSaved: saved,
            pagesVisited: pagesVisited,
            targetJobs: RESULTS_WANTED,
            maxPages: MAX_PAGES,
            uniqueJobUrls: seenJobUrls.size,
            uniquePageUrls: seenPageUrls.size,
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

