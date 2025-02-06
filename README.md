# Collaborative Data Engineering Repository

## Overview
Welcome to the **Collaborative Data Engineering for Violence against Women**! This project is designed to incorporate datasets regarding violence against women and girls into a single source using data engineering techologies.

## Included datasets
* Violence against women and girls. (2020, 12 may). Kaggle. https://www.kaggle.com/datasets/andrewmvd/violence-against-women-and-girls
* Crimes Against Women in India (2001-2021). (2024, 15 august). Kaggle. https://www.kaggle.com/datasets/balajivaraprasad/crimes-against-women-in-india-2001-2021
* Domestic Violence Against Women_Turkey (2008-2021). (2021, 1 april). Kaggle. https://www.kaggle.com/datasets/shahidvuz/violence-against-women-turkey-20082021
* Female genital mutilation (FGM). (2024, march). Unicef. https://data.unicef.org/topic/child-protection/female-genital-mutilation/#data
* Woman Harassment Dataset 2001-21 | Bangladesh. (s. f.). Kaggle. https://www.kaggle.com/datasets/azminetoushikwasi/woman-harassment-dataset-200121-bangladesh
* Number of violence against women cases by types of cases, Malaysia - MAMPU. (s. f.). https://archive.data.gov.my/data/dataset/number-of-violence-against-women-cases-by-types-of-cases-malaysia
* Violencia intrafamiliar. Colombia, años 2015 a 2023. Cifras definitivas | Datos abiertos Colombia. (s. f.). https://www.datos.gov.co/Justicia-y-Derecho/Violencia-intrafamiliar-Colombia-a-os-2015-a-2023-/ers2-kerr/about_data
* Violence against women - Violence against women data - Pacific Data Hub. (s. f.). https://pacificdata.org/data/dataset/violence-against-women-df-vaw/resource/3ac76edc-6f77-4057-a4ee-195e1e0fd73c
* World Bank Open Data. (s. f.). World Bank Open Data. https://data.worldbank.org/indicator/SP.POP.TOTL.FE.IN

## Data
Data is stored in Databricks using Unity Catalog and shared for data science and research purposes, in order to avoid spam and overconsumption of resources if you need the token to connect email the administrator of the repo and ask for it. Admin email: dustycake1@gmail.com

## Contribution Guidelines
We welcome contributions from the community! Please follow these steps:
1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add new feature"
   ```
4. Push to your fork and submit a pull request.

Its important to consider that the code has to be done on pyspark and that it will be run using deltalive tables from databricks, you can follow the folder notebooks in order to get the idea of how to work into a notebook and slowly debug and explain what you are doing. After your notebook is running in your own environment copy only the needed code and copy the structure used in the twin files for each notebook you can find in the folder pipelines.
All the devops and deployment will be done by an admin.
**Important: you must include the dataset and source of the dataset you are adding and it has to be a free to use and open license, the license should also be added**


## License
This project is licensed under the Apache 2.0 license.

## Contact
For any questions, feel free to open an issue or reach out via email at dustycake1@gmail.com.

