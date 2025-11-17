# 🎯 Totaljobs Job Scraper - Complete Job Data Extraction

<div align="center">

![Totaljobs Scraper](https://img.shields.io/badge/Apify-Actor-blue?style=for-the-badge&logo=apify)
![Job Scraping](https://img.shields.io/badge/Job_Scraping-Automated-green?style=for-the-badge)
![Data Extraction](https://img.shields.io/badge/Data_Extraction-Complete-orange?style=for-the-badge)

**Extract comprehensive job listings from Totaljobs.com with intelligent pagination and detailed information capture**

[🚀 Run on Apify](https://apify.com/) • [📖 Documentation](#-documentation) • [💡 Examples](#-usage-examples)

</div>

---

## 🌟 Why Choose Totaljobs Job Scraper?

**Totaljobs Job Scraper** is your complete solution for automated job data extraction from one of the UK's largest job boards. Whether you're building a job search platform, conducting market research, or gathering recruitment intelligence, this scraper delivers structured, comprehensive job data with enterprise-grade reliability.

### ✨ Key Benefits

- **📊 Complete Data Coverage** - Extract titles, companies, locations, salaries, descriptions, and metadata
- **🔄 Intelligent Automation** - Handles pagination, retries, and anti-bot measures automatically
- **⚡ High Performance** - Optimized for speed with smart rate limiting and session management
- **🛡️ Enterprise Ready** - Built-in proxy rotation and error handling for reliable operation
- **📈 Scalable** - Process thousands of jobs with configurable limits and filtering
- **🎯 Flexible Search** - Keyword, location, and category-based job discovery

---

## 🚀 Quick Start

### Basic Job Search
```json
{
  "keyword": "software engineer",
  "location": "London",
  "results_wanted": 100
}
```

### Advanced Configuration
```json
{
  "keyword": "data scientist",
  "location": "Manchester",
  "results_wanted": 500,
  "collectDetails": true,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

**🎯 Ready to scrape? [Start your first run now!](https://apify.com/)**

---

## 📋 Features

<table>
<tr>
<td>

### 🔍 Smart Data Extraction
- **Job Titles & Companies** - Accurate extraction with fallback methods
- **Location Data** - City, region, and postcode information
- **Salary Information** - Range, type, and currency details
- **Job Descriptions** - Full HTML and plain text versions
- **Posting Dates** - Relative and absolute timestamps
- **Job Categories** - Industry and role classifications

</td>
<td>

### ⚙️ Advanced Automation
- **Intelligent Pagination** - Automatic page navigation and discovery
- **Session Management** - Smart session rotation for reliability
- **Rate Limiting** - Built-in delays to respect website limits
- **Error Recovery** - Automatic retries with exponential backoff
- **Anti-Bot Evasion** - Multiple techniques to avoid detection
- **Proxy Integration** - Residential proxy support for scale

</td>
</tr>
</table>

---

## 📥 Input Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `keyword` | `string` | No | `"admin"` | Primary search term (e.g., "software engineer", "marketing manager") |
| `location` | `string` | No | `""` | Geographic filter (e.g., "London", "Manchester", "Birmingham") |
| `category` | `string` | No | `""` | Job category or industry filter |
| `startUrl` | `string` | No | - | Custom Totaljobs URL to begin scraping |
| `url` | `string` | No | - | Alternative custom URL parameter |
| `results_wanted` | `integer` | No | `100` | Target number of jobs to collect (1-10000) |
| `max_pages` | `integer` | No | `10` | Maximum search pages to process |
| `collectDetails` | `boolean` | No | `true` | Fetch full job descriptions from detail pages |
| `proxyConfiguration` | `object` | No | - | Proxy settings for enhanced reliability |

### 🔧 Configuration Examples

#### Entry-Level Jobs Search
```json
{
  "keyword": "graduate",
  "location": "London",
  "results_wanted": 200,
  "max_pages": 15
}
```

#### Senior Management Positions
```json
{
  "keyword": "director",
  "category": "management",
  "results_wanted": 50,
  "collectDetails": true
}
```

#### Custom URL Scraping
```json
{
  "startUrl": "https://www.totaljobs.com/jobs/it",
  "results_wanted": 300,
  "max_pages": 20
}
```

#### Large-Scale Data Collection
```json
{
  "keyword": "engineer",
  "results_wanted": 1000,
  "max_pages": 50,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "apifyProxyCountry": "GB"
  }
}
```

---

## 📤 Output Data Schema

The scraper produces structured JSON records optimized for data analysis and integration.

### Core Job Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `title` | `string` | Job position title | `"Senior Software Engineer"` |
| `company` | `string` | Hiring organization | `"TechCorp Solutions Ltd"` |
| `location` | `string` | Job location details | `"London, Greater London"` |
| `salary` | `string` | Compensation information | `"£50,000 - £70,000 per annum"` |
| `date_posted` | `string` | Posting timestamp | `"2 days ago"` |
| `job_url` | `string` | Direct job link | `"https://www.totaljobs.com/job/..."` |

### Extended Information

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `job_type` | `string` | Employment type | `"Full-time"` |
| `job_category` | `string` | Industry category | `"Information Technology"` |
| `description_html` | `string` | Full HTML description | `"<div><p>We are seeking..."` |
| `description_text` | `string` | Plain text description | `"We are seeking a talented..."` |

### 📊 Sample Output Record

```json
{
  "title": "Senior Full Stack Developer",
  "company": "Digital Innovations Ltd",
  "location": "Manchester, Greater Manchester",
  "salary": "£45,000 - £65,000 per annum",
  "date_posted": "1 day ago",
  "job_type": "Full-time",
  "job_category": "Information Technology",
  "description_html": "<div><p>Join our dynamic team as a Senior Full Stack Developer...</p></div>",
  "description_text": "Join our dynamic team as a Senior Full Stack Developer...",
  "job_url": "https://www.totaljobs.com/job/senior-full-stack-developer/digital-innovations-job12345"
}
```

---

## 🎯 Usage Examples

### Basic API Integration

#### REST API Access
```bash
# Get results as JSON
curl "https://api.apify.com/v2/acts/YOUR-ACTOR-ID/runs/YOUR-RUN-ID/dataset/items?format=json"

# Export as CSV
curl "https://api.apify.com/v2/acts/YOUR-ACTOR-ID/runs/YOUR-RUN-ID/dataset/items?format=csv"
```

#### Python Integration
```python
import requests

# Fetch job data
response = requests.get(
    'https://api.apify.com/v2/acts/YOUR-ACTOR-ID/runs/YOUR-RUN-ID/dataset/items?format=json',
    params={'token': 'YOUR-API-TOKEN'}
)

jobs = response.json()

# Process and analyze
for job in jobs:
    print(f"📋 {job['title']} at {job['company']}")
    print(f"📍 Location: {job['location']}")
    print(f"💰 Salary: {job['salary']}")
    print("---")
```

#### JavaScript/Node.js
```javascript
const Apify = require('apify');

async function processJobs() {
    const run = await Apify.call('YOUR-ACTOR-ID', {
        keyword: 'javascript developer',
        location: 'London',
        results_wanted: 50
    });

    const dataset = await Apify.openDataset(run.defaultDatasetId);
    const jobs = await dataset.getData().then(data => data.items);

    jobs.forEach(job => {
        console.log(`${job.title} - ${job.company} (${job.location})`);
    });
}

processJobs();
```

---

## ⚙️ Advanced Configuration

### Proxy Settings for Scale

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "apifyProxyCountry": "GB"
  }
}
```

### Performance Optimization

| Setting | Recommended | Description |
|---------|-------------|-------------|
| `results_wanted` | 100-500 | Balance data needs with processing time |
| `max_pages` | 10-25 | Prevent excessive crawling |
| `collectDetails` | `true` | Get complete job information |

### Error Handling & Reliability

- **Automatic Retries** - Failed requests are retried with smart backoff
- **Session Rotation** - Fresh sessions prevent blocking
- **Rate Limiting** - Respectful delays between requests
- **Circuit Breaker** - Automatic failure detection and recovery

---

## 💼 Use Cases & Applications

### 🎯 Recruitment & HR
- **Talent Pipeline Building** - Identify qualified candidates across regions
- **Market Intelligence** - Track job market trends and salary ranges
- **Competitor Analysis** - Monitor hiring patterns of industry peers

### 📊 Market Research
- **Industry Analysis** - Study job market demand by sector and location
- **Salary Benchmarking** - Compare compensation across roles and companies
- **Geographic Insights** - Understand regional job market dynamics

### 🤖 Automation & Integration
- **Job Board Aggregation** - Combine data from multiple sources
- **Alert Systems** - Monitor new job postings in specific areas
- **Data Enrichment** - Enhance CRM and applicant tracking systems

### 📈 Business Intelligence
- **Workforce Planning** - Forecast hiring needs based on market data
- **Economic Indicators** - Track employment trends and opportunities
- **Career Development** - Identify in-demand skills and roles

---

## 🔒 Compliance & Best Practices

### Responsible Scraping
- **Rate Limiting** - Built-in delays respect website performance
- **Session Management** - Mimics human browsing patterns
- **Error Recovery** - Graceful handling of temporary issues

### Data Usage Guidelines
- **Terms Compliance** - Adhere to Totaljobs.com terms of service
- **Privacy Respect** - Handle personal data appropriately
- **Legal Compliance** - Ensure usage complies with applicable laws

### Performance Considerations
- **Resource Management** - Efficient memory and network usage
- **Scalability** - Designed for high-volume data collection
- **Monitoring** - Comprehensive logging and error tracking

---

## 🆘 Support & Resources

### Getting Help
- **📧 Support** - Contact via Apify platform for technical assistance
- **🐛 Bug Reports** - Report issues through the Apify console
- **💡 Feature Requests** - Suggest improvements and new capabilities

### Documentation
- **📖 API Reference** - Complete parameter and output documentation
- **🎯 Examples** - Sample configurations for common use cases
- **🔧 Configuration Guide** - Advanced setup and optimization tips

### Updates & Maintenance
- **🔄 Regular Updates** - Continuous improvements and compatibility updates
- **📢 Changelog** - Track new features and bug fixes
- **🛡️ Reliability** - Enterprise-grade stability and performance

---

<div align="center">

## 🚀 Ready to Extract Job Data?

**[Start Scraping Jobs Now](https://apify.com/)**

**Extract • Analyze • Integrate**

*Built for reliability, optimized for performance*

---

**🏷️ Keywords:** job scraper, recruitment data, job listings, employment data, job market analysis, automated scraping, HR data, job search automation, career data, workforce intelligence

</div>