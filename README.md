# Instacart Grocery Data Analytics

![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Scala: 2.12+](https://img.shields.io/badge/Scala-2.12%2B-red.svg)
![Apache Spark](https://img.shields.io/badge/Spark-2.4%2B-orange.svg)

This project leverages Scala and Apache Spark to perform large-scale data analytics on the Instacart grocery shopping dataset. The analysis focuses on understanding customer purchasing patterns, product associations, and reorder behaviors to help improve online grocery services.

This project uses the [Instacart Online Grocery Basket Analysis Dataset](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset) from Kaggle, which contains detailed information about customer orders. By leveraging distributed computing, the project efficiently processes large datasets, providing actionable insights that can be used by retailers to optimize marketing strategies and improve customer satisfaction.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Project Structure](#project-structure)
- [Data Modeling](#data-modeling)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Introduction

Instacart Grocery Data Analytics is a Scala-based project that utilizes Apache Spark to analyze millions of grocery orders. The primary goal is to uncover insights into customer behaviors, such as frequently bought together items, repeat purchase patterns, and segmenting customers based on their shopping habits.

By leveraging distributed computing, the project efficiently processes large datasets, providing actionable insights that can be used by retailers to optimize marketing strategies and improve customer satisfaction.

## Features

- **Scalable Data Processing**: Analyze large datasets using Apache Spark for distributed computing.
- **Customer Segmentation**: Group customers by purchasing behavior and order frequency.
- **Market Basket Analysis**: Identify frequently bought together items using association rules.
- **Reorder Patterns**: Analyze repeat purchases and time intervals between orders.
- **Data Visualization**: Use Spark's integration with visualization libraries to generate insightful plots.

## Project Structure

```plaintext
Instacart-Grocery-Data-Analytics/
│
├── data/                   # Dataset files (e.g., CSV)
├── src/                    # Scala source code
│   └── main/               # Main application code
│       └── scala/          # Scala package
│           └── analytics/  # Analytics code and transformations
├── notebooks/              # Zeppelin or Jupyter notebooks for exploratory analysis
├── results/                # Analysis results
├── build.sbt               # SBT build configuration
├── README.md               # Project documentation
└── LICENSE                 # License file

```

## Data Modeling

In Instacart Grocery data analytics project, I have implemented a dimensional model using a star schema. Transforming data from multiple CSV files into fact and dimension tables. The star schema is designed with a central fact table, fact_order_products, which stores transactional data, surrounded by dimension tables such as dim_products and dim_orders that provide contextual information. This structured design allows for efficient querying and simplifies data analysis, making it a valuable asset for gaining insights in the Instacart data project.

To create below diagram I used online ER diagram website: https://dbdiagram.io

<img width="1179" alt="Data_Modeling" src="https://github.com/user-attachments/assets/7c328b9d-9fb5-4b03-8294-6288295616eb">



## Acknowledgements

- [Instacart](https://www.instacart.com/) for providing the dataset.
- [Kaggle](https://www.kaggle.com/) for hosting the Instacart dataset and providing a platform for data science competitions.
- [Apache Spark](https://spark.apache.org/) for its powerful distributed processing engine.
- [Scala](https://www.scala-lang.org/) for providing a robust language for data engineering.
