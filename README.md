# Olist E-commerce: Seller Management & Data Pipeline Solution

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-elephant?logo=postgresql&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-Storage_%26_BigQuery-4285F4?logo=google-cloud&logoColor=white)
![Looker Studio](https://img.shields.io/badge/Looker-Studio-EA4335?logo=looker&logoColor=white)
![Scikit-Learn](https://img.shields.io/badge/Sklearn-Machine%20Learning-orange?logo=scikitlearn&logoColor=white)

## 1. Problem Statement

### 1.1. Context
This project focuses on **Olist Store**, a B2B2C (Business-to-Business-to-Consumer) e-commerce model in Brazil. Olist acts as a connector, linking small merchants (Sellers) to major e-commerce marketplaces, enabling them to reach customers on a national scale.

### 1.2. Pain Point
Olist's business model relies entirely on the **quality of its Sellers**, as it does not own the inventory.
* **The Issue:** If a Seller ships late, packs poorly, or cancels orders, customer satisfaction drops, directly impacting Olist's brand reputation.
* **The Challenge:** There is a need for an automated system to fairly evaluate, classify, and manage thousands of Sellers, replacing inefficient manual control methods.

---

## 2. Solution Approach

The project approaches the problem through two key pillars:
1.  **Voice of Customer (NLP):** Leveraging Natural Language Processing to analyze review content, distinguishing between dissatisfaction caused by **Logistics** vs. **Product/Seller** issues.
2.  **Seller Scoring & Segmentation:** Building a Data Mining process to score performance and cluster seller personas, leading to specific governance strategies.

---

## 3. Implementation Details

### 3.1. System Architecture & Data Pipeline


**Workflow:**
`9 Raw CSV Files` → `Staging (PostgreSQL)` → `Data Warehouse / Star Schema (PostgreSQL)` → `Google Cloud Storage (Backup)` → `BigQuery (Analytics)` → `Looker Studio (Dashboard)`

* **Ingestion Strategy:** Full Load (Truncate & Load) to ensure data consistency during the development phase.

### 3.2. Data Cleaning & Standardization
Raw data undergoes rigorous processing before entering the Database:
* **Reviews:**
    * *Deduplication:* Kept only the most recent review for each order.
    * *Missing Values:* Replaced NaNs with empty strings.
* **Orders:**
    * *Status Filtering:* Kept only valid orders based on `BUSINESS_RULES` (delivered, shipped, etc.).
    * *Logic Validation:* Removed erroneous records (e.g., Purchase Date > Delivery Date, Estimated Date < Purchase Date).
* **Products:**
    * *Imputation:* Filled 'unknown' for missing categories; 0 for missing descriptions; Median values for missing dimensions/weight.

### 3.3. Data Modeling
Transformed data into a **Star Schema** optimized for analytical queries:
* **Fact Tables:** `fact_orders` (Transactional info), `fact_order_items` (Product details - Bridge Table).
* **Dimension Tables:** `dim_date`, `dim_customers`, `dim_products`, `dim_sellers`.

### 3.4. NLP Analysis (Topic Modeling)
Utilized **LDA (Latent Dirichlet Allocation)** to uncover hidden themes in ~98,500 reviews.
* **Distribution:** ~15% Negative (1-2 stars), ~76% Positive, remainder Neutral.
* **Preprocessing:** Lowercase conversion, Special Character/Number removal, Tokenization, Stop Word removal (Portuguese).
* **LDA Training:** Vectorization, filtering words appearing too frequently (>90%) or too rarely (<25%).
* **Key Findings:** Identified 4 main complaint topics:
    1.  Late delivery / Item not received.
    2.  Wrong item sent / Missing parts.
    3.  Poor product quality.
    4.  Defective product / Return request.

### 3.5. Seller Evaluation & Classification System (Core Module)

#### 3.5.1. Fairness Engine (Noise & Bias Filtering)
To ensure fair evaluation for Sellers, the system applies the following filters:
1.  **Ghost Seller Filter:** Retained only Sellers active > 60 days, with orders in the last 180 days, and total orders > 2. (Reduced Seller count from 2,986 to 2,018).
2.  **Logistics Bias Removal:** Used NLP to exclude 1-3 star reviews where the fault lay with the shipping carrier, not the Seller.

> **Bias Filtering Efficiency:**
> * Total Negative Reviews: 24,612
> * Reviews Removed (Logistics Fault): **4,449** (Accounted for 18.1% of negative reviews).
> -> *Result: Seller scores reflect actual operational capability.*

#### 3.5.2. Seller Scoring
Adopted a **Hybrid Approach** combining Unsupervised and Supervised Learning:
1.  **Labeling:** Used **K-Means (k=4)** to cluster Sellers based on RFM and operational performance to create synthetic labels.
2.  **Feature Importance:** Used **Random Forest** to learn from K-Means labels and determine the weight of each metric:
    * *Rating (0.3288)* > *GMV (0.2478)* > *Orders (0.2281)* > *Late Rate (0.1142)* > *Prep Time (0.0812)*.
3.  **Final Score Calculation:** Applied a weighted formula on MinMax scaled data to generate a 0-100 score.
    * **Ranking:** Platinum, Gold, Silver, Bronze.

#### 3.5.3. Seller Segmentation
Used **Hierarchical Clustering** to define Seller Personas based on product characteristics:
* *Features:* Item weight, Category diversity, Market reach (Distance), Average Order Value.
* **Optimal Result (k=7):**
    * **Clusters 0, 3, 5:** *Local Lightweights* (Light items, low cost, regional sales) - The dominant group.
    * **Clusters 2, 4:** *National Heavyweights* (Light/Medium items, national reach).
    * **Clusters 1, 6:** *High-Ticket Local* (High value, bulky items, regional sales).

*(Note: Experiments with k=12 showed excessive fragmentation within the Low-Cost group, so k=7 was selected).*

---

## 4. Business Impact & Applications

Based on the analysis, the system deployed governance tables on **Google BigQuery** and Dashboards on **Looker Studio**:

1.  **Risk Alert View:** Automatically detects Sellers with Score < 40 or Late Rate > 30% for warning/suspension.
2.  **VIP Program View:** Identifies "National" Sellers with high revenue to propose shipping subsidy strategies.
3.  **Training Focus View:** Filters Sellers with high GMV but poor operations (Low Review Score) to send automated training materials.
