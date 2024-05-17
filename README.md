# Data Pipeline for Google Play Store App Analysis
The Google Play Store is a vast marketplace with a million of apps catering to various interest and needs of users worldwide. The data from this platform certainly holds enormous potential to drive success in the app development business. This project aims to provide actionable insights by examining key metrics and trends within the Google Play Store App landscape. The target audience of this report is primarily the marketing team or the app developers of app development companies.

## Table of Contents
- [Introduction](#introduction)
- [Dataset](#dataset)
- [Installation](#installation)
- [Results](#results)
- [Contributing](#contributing)
- [Dashboard](#dashboard)

## Introduction
In this project, I designed a Data Pipeline that contains several functions or commands. The first one is a function that contains an ETL flow to take the raw dataset from the database (PostgreSQL) and process it into clean data before analysis. It is then followed by a function that uploads the clean data to the Elasticsearch platform so that it can later be used for data visualization using the Kibana platform. On the Kibana platform, I conducted a simple exploratory data analysis on the dataset, which is a collection of information on applications in the Google Play store. Additionally, I implemented a scheduling system in this data pipeline so that it can continue to run based on a predetermined schedule/time, ensuring that the existing data analysis visualization is always up to date.

## Dataset
The dataset used in this project was sourced from the [Kaggle platform](https://www.kaggle.com/datasets/lava18/google-play-store-apps). In general, this dataset consists of attributes that display information about applications in the Google Play Store, such as application name, category, rating, number of reviews, number of installations, size, type, and other relevant details.

## Installation
To run this project locally, please follow these steps:
1. Clone this repository: `git clone git@github.com:teguhsukmanaa/Simple-Data-Pipeline-For-App-Trend-Analysis.git`
2. Or download the files manually.

## Results
The entire program designed runs smoothly, starting from Docker-Compose execution, retrieving data from PostgreSQL, uploading data to Elasticsearch, to visualizing data with Kibana.

## Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request. Let's collaborate and make this project even better!.

## Dashboard
I have created a dashboard to display the analysis and insights from the dataset. However, as the Kibana platform operates only when the server is turned on, I have provided a screenshot snippet in the images folder within this repository for viewing.

Thank you for visiting this repository! If you have any questions or feedback, please don't hesitate to reach me out.


[teguhsukmanaa](https://github.com/teguhsukmanaa)
