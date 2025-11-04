# Totaljobs Scraper

> **Apify Actor** - Scrape job listings from Totaljobs.com with full detail extraction and pagination support.

This actor automatically scrapes job listings from Totaljobs.com, extracting comprehensive job information including titles, companies, locations, salaries, and full job descriptions. It handles pagination intelligently and provides both summary and detailed data extraction modes.

## ✨ Features

- **Comprehensive Data Extraction**: Captures job titles, companies, locations, salaries, posting dates, and full descriptions
- **Intelligent Pagination**: Automatically follows pagination links to collect all available jobs
- **Flexible Search**: Search by keywords, locations, and categories or provide custom URLs
- **Detail Mode**: Optional deep scraping of individual job pages for complete descriptions
- **Structured Output**: Consistent JSON schema for easy data processing and integration
- **Proxy Support**: Built-in proxy rotation for reliable scraping at scale
- **Rate Limiting**: Intelligent delays and session management to respect website limits

## 📥 Input Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keyword` | string | `"admin"` | Job search keyword (e.g., "software engineer", "marketing manager") |
| `location` | string | `""` | Location filter for job search (leave empty for all locations) |
| `category` | string | `""` | Job category filter (if supported by Totaljobs) |
| `startUrl` | string | - | Custom Totaljobs search URL to start scraping from |
| `url` | string | - | Alternative custom URL parameter |
| `results_wanted` | integer | `100` | Maximum number of jobs to collect (1-1000) |
| `max_pages` | integer | `10` | Maximum number of search result pages to visit |
| `collectDetails` | boolean | `true` | Whether to visit individual job pages for full descriptions |
| `proxyConfiguration` | object | - | Proxy settings for reliable scraping |

### Input Examples

**Basic keyword search:**
```json
{
  "keyword": "software engineer",
  "location": "London",
  "results_wanted": 50
}
```

**Custom URL scraping:**
```json
{
  "startUrl": "https://www.totaljobs.com/jobs/admin",
  "collectDetails": true,
  "results_wanted": 25
}
```

**Category-specific search:**
```json
{
  "keyword": "developer",
  "category": "IT",
  "max_pages": 5,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

## 📤 Output Schema

The actor outputs structured JSON records to the Apify dataset. Each record contains:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `title` | string | Job title | `"Senior Software Engineer"` |
| `company` | string | Company name | `"TechCorp Ltd"` |
| `location` | string | Job location | `"London, UK"` |
| `salary` | string | Salary information | `"£50,000 - £70,000 per annum"` |
| `date_posted` | string | When the job was posted | `"2 days ago"` |
| `job_type` | string | Employment type | `"Full-time"` |
| `job_category` | string | Job category | `"Information Technology"` |
| `description_html` | string | Full job description (HTML) | `"<p>We are looking for..."` |
| `description_text` | string | Plain text description | `"We are looking for a senior software engineer..."` |
| `job_url` | string | Direct link to job posting | `"https://www.totaljobs.com/job/..."` |

### Sample Output Record

```json
{
  "title": "Senior Software Engineer",
  "company": "TechCorp Ltd",
  "location": "London, Greater London",
  "salary": "£60,000 - £80,000 per annum",
  "date_posted": "3 days ago",
  "job_type": "Full-time",
  "job_category": "Information Technology",
  "description_html": "<div><p>We are seeking a Senior Software Engineer to join our dynamic team...</p></div>",
  "description_text": "We are seeking a Senior Software Engineer to join our dynamic team...",
  "job_url": "https://www.totaljobs.com/job/senior-software-engineer/techcorp-job12345"
}
```

## 🚀 Usage

### Basic Usage

1. **Set up the actor** in your Apify account
2. **Configure input parameters** (see examples above)
3. **Run the actor** and wait for completion
4. **Download results** from the dataset in JSON, CSV, or Excel format

### Advanced Configuration

For large-scale scraping, configure proxy settings:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "countryCode": "GB"
  },
  "results_wanted": 500,
  "max_pages": 50
}
```

### Integration Examples

**Download as JSON:**
```bash
curl "https://api.apify.com/v2/acts/YOUR-ACTOR-ID/runs/YOUR-RUN-ID/dataset/items?format=json"
```

**Process with Python:**
```python
import requests

response = requests.get('https://api.apify.com/v2/acts/YOUR-ACTOR-ID/runs/YOUR-RUN-ID/dataset/items?format=json')
jobs = response.json()

for job in jobs:
    print(f"{job['title']} at {job['company']} - {job['location']}")
```

## ⚙️ Configuration

### Proxy Settings

For best results, always use proxy configuration:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Performance Tuning

- **results_wanted**: Start with smaller numbers (50-100) for testing
- **max_pages**: Limit to prevent excessive crawling (10-20 recommended)
- **collectDetails**: Set to `false` for faster summary-only scraping

### Error Handling

The actor includes automatic retry logic and handles:
- Network timeouts
- Rate limiting (429 errors)
- Temporary blocks (403 errors)
- Session rotation for reliability

## 📋 Notes & Limitations

- **Rate Limits**: Respects Totaljobs.com's terms of service with intelligent delays
- **Data Freshness**: Job listings are scraped in real-time
- **Geographic Coverage**: Primarily UK-based job listings
- **Content Changes**: Website structure may change; actor includes fallback selectors
- **Legal Compliance**: Ensure compliance with Totaljobs.com terms and applicable laws

## 🆘 Support

- **Issues**: Report bugs via Apify platform
- **Documentation**: Full API reference available in actor details
- **Updates**: Actor is regularly maintained for compatibility

---

**Built for Apify Platform** | **Version**: 1.0.0 | **Last Updated**: November 2025