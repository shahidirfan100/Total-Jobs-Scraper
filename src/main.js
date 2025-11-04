// TotalJobs scraper - Production-ready implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

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
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;

        // Build start URL from keyword/location or use provided URL
        const buildStartUrl = (kw, loc, cat) => {
            const base = 'https://www.totaljobs.com/jobs';
            if (!kw && !loc && !cat) return `${base}/admin`;
            const u = new URL(base + (kw ? `/${encodeURIComponent(kw)}` : ''));
            if (loc) u.searchParams.set('Location', loc);
            if (cat) u.searchParams.set('Category', cat);
            return u.href;
        };

        const initial = [];
        if (startUrl) initial.push(startUrl);
        else if (url) initial.push(url);
        else initial.push(buildStartUrl(keyword, location, category));

        log.info(`TotalJobs scraper started with ${initial.length} start URL(s)`);
        log.info(`Target: ${RESULTS_WANTED} jobs, max ${MAX_PAGES} pages, collectDetails: ${collectDetails}`);

        // Proxy configuration
        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : undefined;

        let saved = 0;
        let pagesVisited = 0;
        const seenUrls = new Set();

        await Dataset.open('totaljobs-jobs');

        // Stealth best practices: aggressive session rotation, lower concurrency, human-like delays
        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 5,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: 50,
                sessionOptions: {
                    maxUsageCount: 3, // aggressive rotation
                    maxErrorScore: 1,
                },
            },
            maxConcurrency: 2, // lower concurrency for stealth
            minConcurrency: 1,
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 60,
            
            // Pre-navigation hook for stealth headers
            preNavigationHooks: [
                async ({ request, session }, gotoOptions) => {
                    // Realistic referer chain
                    const referers = [
                        'https://www.google.com/',
                        'https://www.google.co.uk/search?q=admin+jobs+uk',
                        'https://www.totaljobs.com/',
                    ];
                    const referer = request.userData?.referer || referers[Math.floor(Math.random() * referers.length)];
                    
                    if (!gotoOptions.headers) gotoOptions.headers = {};
                    Object.assign(gotoOptions.headers, {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Referer': referer,
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': referer.includes('totaljobs') ? 'same-origin' : 'cross-site',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'Cache-Control': 'max-age=0',
                    });
                    
                    // Human-like delay before request (network latency simulation)
                    await randomDelay(200, 800);
                },
            ],

            async requestHandler({ request, $, enqueueLinks, session, log: crawlerLog }) {
                const isDetailPage = /\/job\/[^/]+\/[^/]+-job\d+/.test(request.url);
                const isListPage = /\/jobs\//.test(request.url) && !isDetailPage;

                crawlerLog.info(`Processing ${isDetailPage ? 'DETAIL' : 'LIST'} page: ${request.url}`);

                // LIST PAGE: extract job links and pagination
                if (isListPage) {
                    pagesVisited++;
                    if (pagesVisited > MAX_PAGES) {
                        crawlerLog.info(`Reached max pages limit (${MAX_PAGES})`);
                        return;
                    }

                    // Human-like reading time delay
                    await randomDelay(1500, 3500);

                    // Find all job links: /job/[title]/[company]-job[id]
                    const jobLinks = [];
                    $('a[href^="/job/"]').each((i, el) => {
                        const href = $(el).attr('href');
                        if (href && href.match(/\/job\/[^/]+\/[^/]+-job\d+/)) {
                            const fullUrl = `https://www.totaljobs.com${href}`;
                            if (!seenUrls.has(fullUrl)) {
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
                                    'div:nth-child(2)',
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

                    crawlerLog.info(`Found ${jobLinks.length} unique job links on listing page`);

                    // Enqueue job detail pages
                    if (collectDetails && jobLinks.length > 0) {
                        const toEnqueue = jobLinks.slice(0, RESULTS_WANTED - saved);
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
                    if (saved < RESULTS_WANTED && pagesVisited < MAX_PAGES) {
                        const nextPageLinks = [];
                        
                        // Try numbered pagination links
                        $('a[href*="?page="]').each((i, el) => {
                            const href = $(el).attr('href');
                            if (href && href.match(/page=\d+/)) {
                                nextPageLinks.push(`https://www.totaljobs.com${href.startsWith('/') ? href : '/' + href}`);
                            }
                        });
                        
                        // Try "Next" button
                        const nextLink = $('a:contains("Next")').first().attr('href');
                        if (nextLink) {
                            nextPageLinks.push(`https://www.totaljobs.com${nextLink.startsWith('/') ? nextLink : '/' + nextLink}`);
                        }

                        if (nextPageLinks.length > 0) {
                            const nextPage = nextPageLinks[0];
                            if (!seenUrls.has(nextPage)) {
                                seenUrls.add(nextPage);
                                await enqueueLinks({
                                    urls: [nextPage],
                                    transformRequestFunction: (req) => {
                                        req.userData = { referer: request.url };
                                        return req;
                                    },
                                });
                                crawlerLog.info(`Enqueued next page: ${nextPage}`);
                            }
                        }
                    }
                }

                // DETAIL PAGE: extract full job details
                if (isDetailPage) {
                    if (saved >= RESULTS_WANTED) {
                        crawlerLog.info('Reached results limit, skipping detail page');
                        return;
                    }

                    // Human-like reading time for job detail
                    await randomDelay(2000, 5000);

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
                        crawlerLog.info(`Saved job #${saved}: ${record.title} at ${record.company || 'Unknown'}`);
                    } else {
                        crawlerLog.warning(`Skipped incomplete job: ${request.url}`);
                    }
                }
            },

            // Error handling with exponential backoff
            failedRequestHandler: async ({ request }, error) => {
                log.error(`Request ${request.url} failed after ${request.retryCount} retries: ${error.message}`);
                
                // Check for blocking signals
                if (error.message.includes('403') || error.message.includes('429')) {
                    log.warning(`Possible blocking detected on ${request.url}. Rotating session/proxy.`);
                    await randomDelay(3000, 8000); // exponential backoff with jitter
                }
            },
        });

        await crawler.run(initial);

        log.info(`✅ TotalJobs scraper finished. Saved ${saved} jobs from ${pagesVisited} pages.`);
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
