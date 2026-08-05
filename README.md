# Analysis of Amazon Review Dataset Using Big Data Architecture

**Author:** BOBBILI VAMSHI  


---

## Objective

The main ideology behind this project is to analyze large-scale customer feedback from the Amazon Reviews (1996–2023) dataset using PySpark. The goal is to extract meaningful insights, understand customer sentiments, identify trends, and build data-driven models to support decision-making.

Additionally, by analyzing 25 years of purchase history from Amazon, this project identifies which products have dominated, sold the most, and made the greatest impact over the years.

---

## Architecture Followed

The project follows a distributed big data architecture workflow:
1. **Download & Ingest:** Raw review and metadata files are downloaded.
2. **Data Storage (HDFS):** Data is stored in Hadoop Distributed File System (HDFS) for scalable, fault-tolerant storage.
3. **Data Processing & Analytics (PySpark):** PySpark is used to process, clean, join, and analyze large datasets.
4. **Insights & Visualizations:** Key analytics and data insights are extracted from the processed DataFrames.

---

## Data Source & Specifications

### Primary Download Source
* **Official Website:** [McAuley Lab - UCSD Amazon Reviews Dataset](http://jmcauley.ucsd.edu/data/amazon/)
* **Original Compiler:** Curated by Julian McAuley and colleagues
* **Version Used:** Amazon Reviews Data and Metadata (2023).

### Data Format
The dataset was initially downloaded in JSON format (hierarchical and semi-structured). To prepare the data for efficient Exploratory Data Analysis (EDA) and model training, the raw JSON data was converted into a flat, tabular PySpark DataFrame structure.

---

## Schema & Data Fields

### 1. User Reviews Data Fields
| Field | Type | Explanation |
| :--- | :--- | :--- |
| `rating` | `float` | Rating of the product (from 1.0 to 5.0). |
| `title` | `str` | Title of the user review. |
| `text` | `str` | Text body of the user review. |
| `images` | `list` | List of user-posted images (`small_image_url`, `medium_image_url`, `large_image_url`). |
| `asin` | `str` | ID of the specific product. |
| `parent_asin` | `str` | Parent ID of the product (used to join with item metadata). |
| `user_id` | `str` | ID of the reviewer. |
| `timestamp` | `int` | Time of the review in Unix epoch time. |
| `verified_purchase` | `bool` | User purchase verification status. |
| `helpful_vote` | `int` | Number of helpful votes for the review[. |

### 2. Item Metadata Data Fields
| Field | Type | Explanation |
| :--- | :--- | :--- |
| `main_category` | `str` | Main category/domain of the product. |
| `title` | `str` | Name of the product. |
| `average_rating` | `float` | Rating shown on the product page. |
| `rating_number` | `int` | Number of ratings. |
| `features` | `list` | Bullet-point features of the product. |
| `description` | `list` | Description of the product. |
| `price` | `float` | Price in USD at the time of crawling. |
| `images` | `list` | Product images (`thumb`, `large`, `hi_res`). |
| `videos` | `list` | Product videos including title and URL. |
| `store` | `str` | Store name of the product. |
| `categories` | `list` | Hierarchical categories of the product. |
| `details` | `dict` | Details including materials, brand, sizes, etc. |
| `parent_asin` | `str` | Parent ID of the product. |
| `bought_together` | `list` | Recommended bundles from the website. |

---
## Highlights of PySpark in this Project
* **Distributed Computing Power:** PySpark enables seamless, high-speed processing of massive, multi-gigabyte datasets across distributed clusters using in-memory computing
* **Unified API:** It provides a flexible Python API that combines the simplicity of Python data analysis with the scalability of Apache Spark.
* **Rich DataFrame Ecosystem:** Offers structured PySpark DataFrames that support intuitive, SQL-like data manipulation, filtering, and complex joins.
* **Scalable Data Ingestion:** Easily reads, parses, and converts unstructured or semi-structured data formats like JSON/JSONL into flattened tables.
* **Hadoop Integration:** Seamlessly integrates with HDFS storage systems for reliable, fault-tolerant, and localized big data analysis
you can find the usage of pyspark in the pptx file and aswell as in the code file .
## Using HDFS for Storage

HDFS (Hadoop Distributed File System) serves as the primary distributed storage system for large-scale data processing, offering:
* **Distributed Storage**
* **Large File Optimization**
* **Data Locality**
* **Fault Tolerance & Reliability**

### Commands Used for Ingestion:
```bash
sudo su – hadoop
start-all.sh

# Copy dataset and create target HDFS directory
sudo cp home/documents/amazon_reviews.json ./
hdfs dfs -mkdir -p /user/hadoop/raw_data/
hdfs dfs -put amazon_reviews.json /user/hadoop/raw_data/

# Store file into HDFS with custom block size and replication factor
hdfs dfs -D dfs.replication=1 -D dfs.block.size=1073741824 -put /home/hadoop_user/amazon_reviews.json /raw/amazon_data/



---


