# Analysis of Amazon Review Dataset Using Big Data Architecture

**Author:** BOBBILI VAMSHI  


---

## Objective

The main ideology behind this project is to analyze large-scale customer feedback from the Amazon Reviews (1996–2023) dataset using PySpark[cite: 1]. The goal is to extract meaningful insights, understand customer sentiments, identify trends, and build data-driven models to support decision-making[cite: 1].

Additionally, by analyzing 25 years of purchase history from Amazon, this project identifies which products have dominated, sold the most, and made the greatest impact over the years[cite: 1].

---

## Architecture Followed

The project follows a distributed big data architecture workflow[cite: 1]:
1. **Download & Ingest:** Raw review and metadata files are downloaded[cite: 1].
2. **Data Storage (HDFS):** Data is stored in Hadoop Distributed File System (HDFS) for scalable, fault-tolerant storage[cite: 1].
3. **Data Processing & Analytics (PySpark):** PySpark is used to process, clean, join, and analyze large datasets[cite: 1].
4. **Insights & Visualizations:** Key analytics and data insights are extracted from the processed DataFrames[cite: 1].

---

## Data Source & Specifications

### Primary Download Source
* **Official Website:** [McAuley Lab - UCSD Amazon Reviews Dataset](http://jmcauley.ucsd.edu/data/amazon/)[cite: 1]
* **Original Compiler:** Curated by Julian McAuley and colleagues[cite: 1].
* **Version Used:** Amazon Reviews Data and Metadata (2023)[cite: 1].

### Data Format
The dataset was initially downloaded in JSON format (hierarchical and semi-structured)[cite: 1]. To prepare the data for efficient Exploratory Data Analysis (EDA) and model training, the raw JSON data was converted into a flat, tabular PySpark DataFrame structure[cite: 1].

---

## Schema & Data Fields

### 1. User Reviews Data Fields
| Field | Type | Explanation |
| :--- | :--- | :--- |
| `rating` | `float` | Rating of the product (from 1.0 to 5.0)[cite: 1]. |
| `title` | `str` | Title of the user review[cite: 1]. |
| `text` | `str` | Text body of the user review[cite: 1]. |
| `images` | `list` | List of user-posted images (`small_image_url`, `medium_image_url`, `large_image_url`)[cite: 1]. |
| `asin` | `str` | ID of the specific product[cite: 1]. |
| `parent_asin` | `str` | Parent ID of the product (used to join with item metadata)[cite: 1]. |
| `user_id` | `str` | ID of the reviewer[cite: 1]. |
| `timestamp` | `int` | Time of the review in Unix epoch time[cite: 1]. |
| `verified_purchase` | `bool` | User purchase verification status[cite: 1]. |
| `helpful_vote` | `int` | Number of helpful votes for the review[cite: 1]. |

### 2. Item Metadata Data Fields
| Field | Type | Explanation |
| :--- | :--- | :--- |
| `main_category` | `str` | Main category/domain of the product[cite: 1]. |
| `title` | `str` | Name of the product[cite: 1]. |
| `average_rating` | `float` | Rating shown on the product page[cite: 1]. |
| `rating_number` | `int` | Number of ratings[cite: 1]. |
| `features` | `list` | Bullet-point features of the product[cite: 1]. |
| `description` | `list` | Description of the product[cite: 1]. |
| `price` | `float` | Price in USD at the time of crawling[cite: 1]. |
| `images` | `list` | Product images (`thumb`, `large`, `hi_res`)[cite: 1]. |
| `videos` | `list` | Product videos including title and URL[cite: 1]. |
| `store` | `str` | Store name of the product[cite: 1]. |
| `categories` | `list` | Hierarchical categories of the product[cite: 1]. |
| `details` | `dict` | Details including materials, brand, sizes, etc.[cite: 1] |
| `parent_asin` | `str` | Parent ID of the product[cite: 1]. |
| `bought_together` | `list` | Recommended bundles from the website[cite: 1]. |

---

## Using HDFS for Storage

HDFS (Hadoop Distributed File System) serves as the primary distributed storage system for large-scale data processing[cite: 1], offering:
* **Distributed Storage**[cite: 1]
* **Large File Optimization**[cite: 1]
* **Data Locality**[cite: 1]
* **Fault Tolerance & Reliability**[cite: 1]

### Commands Used for Ingestion:
```bash
sudo su – hadoop[cite: 1]
start-all.sh[cite: 1]

# Copy dataset and create target HDFS directory
sudo cp home/documents/amazon_reviews.json ./[cite: 1]
hdfs dfs -mkdir -p /user/hadoop/raw_data/[cite: 1]
hdfs dfs -put amazon_reviews.json /user/hadoop/raw_data/[cite: 1]

# Store file into HDFS with custom block size and replication factor
hdfs dfs -D dfs.replication=1 -D dfs.block.size=1073741824 -put /home/hadoop_user/amazon_reviews.json /raw/amazon_data/[cite: 1]

## Highlights of PySpark in this Project

* **Distributed Computing Power:** PySpark enables seamless, high-speed processing of massive, multi-gigabyte datasets across distributed clusters using in-memory computing[cite: 1].
* **Unified API:** It provides a flexible Python API that combines the simplicity of Python data analysis with the scalability of Apache Spark[cite: 1].
* **Rich DataFrame Ecosystem:** Offers structured PySpark DataFrames that support intuitive, SQL-like data manipulation, filtering, and complex joins[cite: 1].
* **Scalable Data Ingestion:** Easily reads, parses, and converts unstructured or semi-structured data formats like JSON/JSONL into flattened tables[cite: 1].
* **Hadoop Integration:** Seamlessly integrates with HDFS storage systems for reliable, fault-tolerant, and localized big data analysis[cite: 1].

---
