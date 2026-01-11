# Olist E-commerce: End-to-End Data Pipeline & Seller Strategy

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-elephant?logo=postgresql&logoColor=white)
![Scikit-Learn](https://img.shields.io/badge/Sklearn-Machine%20Learning-orange?logo=scikitlearn&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 📖 Executive Summary
**Olist** operates as a B2B2C marketplace connector in Brazil, linking small merchants (Sellers) to major e-commerce platforms. 
* **The Problem:** As a marketplace without direct inventory control, Olist's brand reputation relies entirely on the performance of third-party sellers. Bad sellers lead to customer churn.
* **The Solution:** This project builds an automated **Data Pipeline (ETL)** and a **Seller Management System** powered by Machine Learning (NLP & Clustering) to assess, segment, and strategically manage seller partnerships.

---

## 🏗️ System Architecture

The project follows a **Hybrid ETL/ELT** architecture to transform raw transactional data into actionable insights.

![Architecture Diagram](https://via.placeholder.com/800x400?text=Insert+Architecture+Diagram+Here)
*(Please insert your Architecture/ERD Diagram here)*

### 1. Data Engineering Pipeline
* **Source:** 9 Raw CSV tables (Olist Public Dataset).
* **Staging Layer (Python):** * Data cleaning, type casting, and deduplication.
    * Handling NULL values using median/mode imputation.
* **Data Warehouse (PostgreSQL):** * Transformation from **OLTP** to **OLAP (Star Schema)**.
    * Creation of Fact tables (`fact_orders`) and Dimension tables (`dim_sellers`, `dim_products`).
    * **Integration:** NLP results are materialized back into the warehouse (`warehouse.nlp_bad_review`) for reporting.

### 2. Tech Stack
* **Language:** Python 3.x
* **Database:** PostgreSQL
* **Data Processing:** Pandas, NumPy, SQLAlchemy.
* **Machine Learning:** Scikit-learn (K-Means, Random Forest, PCA), NLTK (LDA Topic Modeling).
* **Visualization:** Matplotlib, Seaborn.

---

## 🔍 Key Modules & Analysis

### Module 1: Voice of Customer (NLP Analysis)
**Objective:** Understand the root causes of negative reviews (1-2 stars) to verify business problems.
* **Technique:** Latent Dirichlet Allocation (LDA) for Topic Modeling.
* **Key Findings:**
    * **75%** of negative feedback is related to **Product Quality**.
    * **25%** is related to **Logistics/Shipping Delays**.
* **Conclusion:** Validated the hypothesis that rigorous Seller Quality Control is essential.

### Module 2: Seller Management System (Core)
Developed a comprehensive system to evaluate sellers not just by sales volume, but by a combination of **Performance** and **Persona**.

#### A. The "Fair" Scoring Engine
Ensure fairness by removing factors outside the seller's control before scoring:
* **Ghost Seller Filtering:** Removed inactive sellers based on operation time and order count.
* **NLP Review Filtering:** Excluded 1-star reviews caused by *Shipping/Logistics* (detected via NLP), ensuring sellers aren't penalized for delivery partner errors.
* **Feature Importance:** Used **Random Forest** to identify `Late_shipment_rate` and `GMV` as the top drivers for seller scores.

#### B. Seller Profiling (Clustering)
Used **K-Means Clustering** (K=7) to segment sellers based on behavior (`Product Weight`, `Category Diversity`, `Geographic Reach`).
* **Identified Personas:**
    * 🧊 **The Local Lightweights:** Sellers with low-cost, light items serving local regions (Dominant group).
    * 🏗 **The National Heavyweights:** Sellers shipping bulky/heavy items nationwide.
    * 💎 **High-Ticket Niche:** Sellers with high average order value but lower volume.

#### C. Strategic Matrix (Results)
By combining **Performance Score** vs. **Seller Persona**, we propose the following strategies:

![Seller Heatmap](https://via.placeholder.com/800x400?text=Insert+Seller+Matrix+Heatmap)
*(Please insert your Heatmap/Boxplot from the notebook here)*

| Segment | Strategy |
| :--- | :--- |
| **High Performing + National Heavyweights** | **Partnership:** Offer subsidized shipping rates for heavy cargo to maintain loyalty. |
| **High Ticket Niche** | **VIP Support:** Prioritize insurance and dedicated support, as churn here causes high revenue loss. |
| **Low Performing (High Late Rate)** | **Automation:** Trigger automatic warnings or account suspension if scores drop below threshold. |

---

## 📂 Project Structure

```text
Olist_Project/
│
├── data/                   # Raw and Processed Data (Not included in repo)
├── notebooks/              # Jupyter Notebooks for Analysis
│   ├── nlp_analysis.ipynb          # NLP: Bad Review Analysis
│   ├── nlp_review_good.ipynb       # NLP: Good Review Analysis
│   └── seller_management.ipynb     # RFM Analysis, Clustering & Scoring
│
├── src/                    # Source Code for ETL Pipeline
│   ├── config.py           # Database configurations
│   ├── data_cleaning.py    # Staging logic
│   ├── data_transformation.py # Warehousing logic (Star Schema)
│   └── data_aggregation.py # Aggregation logic
│
├── images/                 # Diagrams and Charts for README
├── requirements.txt        # Python Dependencies
└── README.md               # Project Documentation