# **ETL Pipeline: Advanced Data Handling and Automation**

Welcome to the **ETL Pipeline** repository! This project is designed to extract, transform, and load data efficiently using advanced data-handling strategies. It ensures high compatibility, scalability, and automation, meeting modern data engineering best practices.

---

## **Features**
- **Data Extraction**: Reads data from CSV files with comprehensive error handling.
- **Data Transformation**: Cleanses, standardizes, and enriches datasets for analysis (e.g., date standardization, null handling, derived metrics).
- **Data Loading**: Loads transformed data into a PostgreSQL database.
- **Role-Based Access Control**: Implements PostgreSQL roles for secure data access (`etl_role`, `readonly_role`, etc.).
- **Automation**: Scheduler automates the ETL pipeline execution.
- **Logging**: Detailed logging for monitoring pipeline status and debugging issues.
- **Testing**: Unit and integration tests ensure pipeline reliability.

---

## **Repository Structure**
```
/
├── data/                      # Sample data files (CSV)
├── function/
│   ├── etl_pipeline.py        # Main ETL pipeline code
│   ├── etl_scheduler.py       # Scheduler for automation
│   ├── test_etl_pipeline.py   # Unit tests for the pipeline
│   ├── test_integration_etl.py# Integration tests for the pipeline
├── logs/                      # Log files for ETL and tests
├── config.ini                 # Configuration file (database credentials)
├── README.md                  # Project documentation (this file)
```

---

## **Setup**

### **1. Clone the Repository**
```bash
git clone https://github.com/your-username/etl-pipeline.git
cd etl-pipeline
```

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3. Configure Database**
- Create a PostgreSQL database (e.g., `retailpro_db`).
- Add your database credentials to `config.ini`:
  ```
  [database]
  user = your_username
  password = your_password
  host = localhost
  port = 5432
  database = retailpro_db
  ```

### **4. Update File Paths**
Absolute paths are used in the pipeline due to a compatibility issue on the personal system used for development. Update all paths in the code (e.g., `etl_pipeline.py`, `etl_scheduler.py`) to match your local setup.

### **5. Run ETL Pipeline**
```bash
python function/etl_pipeline.py
```

### **6. Automate Pipeline Execution**
To run the pipeline at scheduled intervals:
```bash
python function/etl_scheduler.py
```

---

## **Testing**

### **Run Unit Tests**
```bash
python function/test_etl_pipeline.py
```

### **Run Integration Tests**
```bash
python function/test_integration_etl.py
```

---

## **Usage**
- **Data Input**: Place CSV files in the `/data/` directory.
- **Output**: Transformed data is loaded into the PostgreSQL database.
- **Logs**: Check the `/logs/` directory for ETL and test execution details.

---

## **Key Features**

### **1. Role-Based Security**
- `etl_role`: Full access for ETL operations.
- `readonly_role`: Limited access for analysts.
- Custom roles ensure data security and manageability.

### **2. Data Transformation**
- Standardizes dates, fills missing values, and calculates metrics (e.g., `total_sale`).

### **3. Automation**
- Uses the `schedule` library for automated, timed pipeline execution.

### **4. Testing**
- Comprehensive unit and integration tests validate every stage of the ETL pipeline.

---

## **Contributing**
Contributions are welcome! Please:
1. Fork the repository.
2. Create a new branch (`feature-branch-name`).
3. Submit a pull request for review.

---

## **License**
This project is licensed under the [MIT License](LICENSE).

---

Feel free to add badges, CI/CD instructions, or more details as required!