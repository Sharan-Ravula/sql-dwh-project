# T-SQL Data Warehouse & ETL Automation Project

### 📌 Project Overview

This project is a comprehensive Data Warehousing solution built using **T-SQL** and **Azure Cloud Services**. It was born out of a need to practice enterprise-level SQL development on a macOS environment, where local MS SQL Server Installation (SSMS) is not natively supported.

The project implements a **Medallion Architecture** (Bronze, Silver, Gold) and features a fully automated ETL pipeline using Azure Data Factory to ingest CSV datasets into an **Azure SQL Database**.

---

### 🎓 Acknowledgments

A huge shout-out to `Data with Baraa`. This project was inspired by and built using knowledge from his **30-hour SQL Video Course**.

- **Video Link**: [SQL Server Tutorial - Data with Baraa](https://youtu.be/SSKVgrwhzus)
- **Datasets**: All material and datasets used in this project were provided by Baraa.

---

### 🛠 Tech Stack

- **Database**: `Azure SQL Database` (Serverless)

- **Scripting**: `T-SQL` (MS SQL)

- **IDE**: `Visual Studio Code` (with MSSQL Extension)

- **Orchestration**: `Azure Data Factory` (ADF)

- **Storage**: `Azure Blob Storage`

- **Planning**: `Notion` (Notes/Tasks) & `Draw.io` (Architecture/ERD)

- **Naming-Convention**: `snake-case`

---

### 🏗 Data Architecture & Strategy

- **Extraction**: `File Parsing` from Blob Storage.

- **Method**: `Full Load` using Batch Processing.

- **Load Logic**: `Truncate & Insert` (**SCD Type 1 - Overwrite**).

- **Medallion Layers**:

[Data_Architecture](docs/data_architecture.pdf)

- **Bronze**: Raw data ingestion.
- **Silver**: Cleaned and joined data (using Primary Keys identified in Draw.io).
- **Gold**: Aggregated tables ready for analytics.

---

### 🚀 Setup & Implementation Guide
```text
`You Write the Code` (On your Mac) -> `VS Code Sends the Command` (Over the Internet) 
                ^                                       |
                |                                       v
`VS Code Displays the Result` (Back on your Mac) <- `Azure SQL Executes the Command` (In Cloud)
```

#### 💥 Azure SQL Database & Server Setup

1. 🔍 **Navigate to the Creation Page of Azure Portal**:

    - **Select the Service**: In the search menu you opened (the one showing "Azure SQL"), click on the service named `Azure SQL Database`

    - **Start Creation**: On the main Azure SQL page, click the `+ Create` button to open the configuration form for a new database.

2. ⚙️ **Configure the Database and Server**:

    > This is the most critical part where you set up your server and apply the free offer.

    - **Project Details**:

        - **Subscription**: Select your `Azure subscription`

            > (e.g., the one that came with your free account).
        
        - **Resource group**: Click `Create new` and enter a `simple name`, like SQL-Learning-RG, and click OK.
        
            > A Resource Group is just a folder/container for your Azure resources.
    
    - **Database Details**: Enter a unique name for your `database`

        > e.g., MyLearningDB.

    - **Create the Server**:

        * For the Server field, click `Create new`. A side panel will open.

        * **Server name**: Enter a globally `unique name` 
            
            > e.g., sqlserver-123.
        
        * **Location**: Select a `region close to you`
        
        * **Authentication method**: Select `Use SQL authentication`
        
        * **Server admin login**: Choose a `username` 💡 Write this down.

         > e.g., server_admin. 
        
        * **Password**: Create a strong `password` 💡 Write this down.
        
        * Click OK to create the server.

    - **Apply Free Offer & Networking**:
    
        > This ensures your usage stays within the free limits and allows your Mac to connect.

        - **Apply Free Offer**:

            * Look for an option or banner to Apply offer or select the `General Purpose Serverless compute tier`

            * Ensure the Cost summary card on the right shows the Estimated Cost/Month as `zero or Free`
            
                > The free offer includes 100,000 vCore seconds and 32 GB of storage per month.
        
        - **Networking Tab**:

            * **Click Next**: `Networking`.

            * Under Firewall rules, set `Add current client IP address to Yes`. This adds your Mac's current IP address to the firewall so you can connect from VS Code

            * **Connection Policy**: `Default`

            * **Encrypted connections**: `Minimum TLS version`

        - **Review and Create**: Click `Review + create` and then click Create.

        - **Get Final Connection Details**:

            > Once the deployment finishes (it may take a few minutes):

            * Click Go to resource.

            * On the Overview page for your new server, find and copy the `Server name` 💡 Write this down.

                > (e.g., sqlserver-name.database.windows.net). 

#### 💥 Connect to the Database from Visual Studio Code

1. Open **`Visual Studio Code`** on your Mac.

2. Open the **`Command Palette`** (Cmd + Shift + P).

3. **Type `MS SQL`**: Connect and select it.

4. Choose + **`Create Connection Profile`**.

5. Enter the following details you recorded:

    * **Server Name**: Paste the `server name` you copied from the Azure portal.

    * **Database Name**: Enter the name of your `database` 
        
        >(e.g., MyLearningDB).
    
    * **Authentication Type**: Select `SQL Login`
    
    * **User Name**: Enter the Server `admin` login you created 
        > (e.g., azureuser).
    
    * **Password**: Enter the strong `Password`you created.

> VS Code will establish the connection. You can now start writing and executing T-SQL queries!

#### 💥 Creating BLOB Storage & Container

1. **Basics**:

    - **Subscription**: `Free subscription` we get when we create the account

    - **Resource group**: select the already `existing resource group` which is connected to the database and the server

    - **Storage account name**: `unique name`

    - **Region**: near you `location`

    - **Redundancy**: `Azure Locally Redundant Storage` (**LRS**)

2. **Advanced**:

    - **Require secure transfer for rest api operations**: `checked`

    - **Enable storage account key access**: `checked`

    - **Minimum TLS version**: `Version 1.2`

    - **Access Tier**: `Hot`

3. **Networking**:

    - **Public Network acess**: `enable`

    - **Public network access scope**: `enable from all networks`

    - **Routing preference**: `Microsoft Network Routing`

4. **Encryption Type**: `MMK`

> Once the blob storage is deployed

5. **Create a Container**:

    - Once your storage account is ready, `click Containers` in the left menu.

    - Click + Container and name it like project-data.

    - Upload your CSV files here.

#### 💥 How to give container access to Azure Server using Managed Identity

1. **Enable Identity on your SQL Server**:

    - Go to your `SQL Server` in the Azure Portal.

    - On the left menu, select `Identity` under Security.

    - Turn Status to `On` for "System assigned" and click Save. This gives your server its own "ID badge"

2. **Grant Permissions on the Storage Account**:

    - Go to your `Storage Account` (storageproject).

    - Select `Access Control (IAM)` -> `Add` -> `Add role assignment`

    - Choose the role: `Storage Blob Data Reader`

    - For "Assign access to," choose `Managed Identity`

    - Select your `SQL Server` from the list and click `Review + assign`

#### 💥 Create the Data Factory

1. Search for `Data Factories` in the Azure Portal.

2. Create one named `df-username-imports`

> keep it in the same Region as your storage.

3. Once created, click `Launch Studio`

### 💥 ETL Automation: Converting CSV to SQL Table Data

🛠️ **Phase 1: The Connections (`Linked Services`)**:

> Before you can build anything, the "cables" must be plugged in.

- In ADF Studio, go to the `Manage tab` 

    > bottom-left icon.

- Select `Linked services` -> `+ New`

- **Source**: Search `Azure Blob Storage`. Name it like `AzureBlobStorage1`

- Connect it to your storage account and click `Create`

- **Destination**: Search `Azure SQL Database`. Name it like `AzureSqlDatabase1`

- Enter your server/DB details and click `Create`

🛠️ **Phase 2: The Templates (`Datasets`)**:

> We create the "pockets" (parameters) now so they are ready for the pipeline later.

1. **Part A: The Source (`ds_SourceCSV`)**:

    - Go to the `Author` (Pencil) tab. Click + -> `Dataset` -> `Azure Blob Storage` -> `DelimitedText`

    - Name: `ds_SourceCSV`

        > You can name it anything you like
    
    - **Linked Service**: `AzureBlobStorage1`

    - **Important**: Pick your container (e.g., data), but leave `Directory` and `File` empty. 

    - Check `First row as header`. Click OK.

    - **Parameters Tab**: Click `+ New` twice. Create:

        * `p_FolderName`

        * `p_FileName`

    - **Connection Tab**: 

        * In the `Directory box`, click `"Add dynamic content"` -> select `@dataset().p_FolderName`

        * In the `File box`, click `"Add dynamic content"` -> select `@dataset().p_FileName`

2. **Part B: The Sink (`ds_SQL_Sink`)**:

    - Click `+` -> `Dataset` -> `Azure SQL Database`

    - **Name**: `ds_SQL_Sink`

        > You can name it anything you like

    - **Linked Service**: `AzureSqlDatabase1`

    - **Parameters Tab**: Click `+ New`, Create `p_TableName` and `p_SchemaName`

    - **Connection Tab**: 

        * In the `Table section`, check the `Edit box` (to enter manually).

        * Click the box -> `"Add dynamic content"` -> select `@dataset().p_TableName`

        * Click the box -> `"Add dynamic content"` -> select `@dataset().p_SchemaName`

🛠️ **Phase 3: The Pipeline Logic**:

> Now we build the "brain" to use these templates.

1. Click `+` -> `Pipeline` -> Name it like `pl_BulkCSVImport`

2. **Create Parameter**: Click the empty canvas background -> `Parameters tab` -> `+ New`. Name it `UserSelectedFolder`

3. **The Scout (`Get Metadata`)**:

    * Drag `Get Metadata` onto the canvas.

    * **Settings Tab**: Select `ds_SourceCSV`

    * **In Dataset properties**:

        - set `p_FolderName` to `@pipeline().parameters.UserSelectedFolder`

        - set `p_FileName` to `*.csv` or `*.any-file-extension`

    * **Field list**: Click `+ New` -> choose `Child Items`

4. **The Loop (`ForEach`)**:

    * Drag `ForEach` onto the canvas. `Connect the green arrow from Get Metadata to it`

    * **Settings Tab**: Set `Items` to `@activity('Get Metadata1').output.childItems`

🛠️ **Phase 4: The Engine (`Inside the Loop`)**:

> This is where the actual copying happens.

1. Click the Pencil (`Edit`) icon on the `ForEach` box.

2. Drag a `Copy Data` activity onto the blank canvas.

3. **Source Tab**:

    * Select `ds_SourceCSV`

    * **`p_FolderName`**: `@pipeline().parameters.UserSelectedFolder`

    * **`p_FileName`**: `@item().name`

4. **Sink Tab**:

    * Select `ds_SQL_Sink`

    * **p_TableName**: `@replace(item().name, '.csv', '')`

    * **p_SchemaName**: `(type your unique schema name)`

    * **Table option**: Select `Auto create table`

    * Look for the box labeled `Pre-copy script`

    * **Enter this exact logic**: `TRUNCATE TABLE schemaname.@{replace(item().name, '.csv', '')}` 
        
        > **`IMPORTANT`**: Do not use this code if you are publishing for the first time, only use it when you debug and get every single file successful.
        
        > **`Note`**: This command tells SQL: "Right before you put the new data in, empty out the existing table named `[YourTable]`." This ensures you never have duplicate data if you have to run a debug multiple times.

🏁 **Phase 5: Launch**:

1. **`Validate All`**: Click the checkmark at the top. 
    
    >Should say "No errors found"

2. **`Publish All`**: Click the blue button at the top to save everything.

3. **`Debug`**: Enter your folder name 

    > (e.g., source_crm) and watch the magic happen.

---

## 📂 Repository Structure
```text
sql-dwh-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.png                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture with pdf
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                 # Draw.io file for the data flow diagram with pdf
│   ├── data_layers.pdf                 # Understanding the Medallion Architecture (Bronze, Silver, Gold)
│   ├── data_models.drawio              # Draw.io file for data models (star schema) with pdf (left)
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── silver/                         # Scripts for cleaning, transforming data and quality checks
│   ├── gold/                           # Scripts for creating analytical models and quality checks
│   ├── init_database.sql               # to initialize the database, with commands
│
├── README.md                           # Project overview and instructions
└── LICENSE                             # License information for the repository
```

---

### 📊 Planning Documentation

- **Notion**: Used for tracking the project roadmap, T-SQL syntax notes, and ETL logic.

> Click to open [Notion Document](https://www.notion.so/Data-Warehouse-Project-3073c8d138f080f4bf31ff552297e0c1?source=copy_link)

- **Draw.io**: Used to visualize the Star Schema/Snowflake Schema and determine the specific Join keys (Primary Key/Foreign Key) required for the Silver layer.

---

> Created as part of my Data Engineering learning journey.
