# 📤 Dataset Upload Guide: Using SnowCLI with Dataset_colored_IBAN

## Overview

This guide helps you upload all 1000+ invoice PDFs from your `Dataset_colored_IBAN` directory to Snowflake using **SnowCLI** - the most reliable method for bulk file operations.

## 🚀 **Quick Start (3 Steps)**

### **Step 1: Verify Setup**
```bash
./verify_snowcli_setup.sh
```

### **Step 2: Upload Dataset**
```bash
./snowcli_upload_simple.sh
```

### **Step 3: Test Your Apps**
```bash
streamlit run sis_basic_invoice_app.py
```

## 📋 **Prerequisites**

### **1. SnowCLI Installation**
```bash
# Install SnowCLI
pip install snowflake-cli-labs

# Verify installation
snow --version
```

### **2. SnowCLI Connection Configuration**
```bash
# Add your Snowflake connection
snow connection add

# Test the connection
snow connection test
```

**Connection Setup:** Follow the prompts to configure:
- Account identifier
- Username/password or key-pair authentication
- Default warehouse: `DOC_AI_QS_WH`
- Default database: `DOC_AI_QS_DB`
- Default schema: `DOC_AI_SCHEMA`

### **3. Snowflake Database Setup**
Ensure your Snowflake environment is set up:
```bash
# Run the original setup (if not done)
snow sql --filename docai_invoice_qs_setup.sql

# (Optional) Run enhanced AI features
snow sql --filename cortex_enhancements.sql
```

## 📁 **Available Upload Scripts**

### **🎯 Recommended: Simple Upload**
**File:** `snowcli_upload_simple.sh`
- ✅ **No external dependencies** (no `jq` required)
- ✅ **Simple and reliable**
- ✅ **Clear progress tracking**
- ✅ **Automatic error handling**

```bash
chmod +x snowcli_upload_simple.sh
./snowcli_upload_simple.sh
```

### **🔧 Advanced: Full-Featured Upload**
**File:** `snowcli_bulk_upload.sh`
- ✅ **Advanced verification**
- ✅ **JSON parsing with `jq`**
- ✅ **Detailed progress reports**
- ⚠️ **Requires `jq` installation**

```bash
# Install jq if needed
brew install jq  # macOS
# or: apt-get install jq  # Linux

chmod +x snowcli_bulk_upload.sh
./snowcli_bulk_upload.sh
```

### **🔍 Setup Verification**
**File:** `verify_snowcli_setup.sh`
- ✅ **Comprehensive setup check**
- ✅ **Verifies all prerequisites**
- ✅ **Tests connections and permissions**

```bash
chmod +x verify_snowcli_setup.sh
./verify_snowcli_setup.sh
```

## 📊 **What Gets Uploaded**

### **Source Directory:** `Dataset_colored_IBAN/`
- **1000+ invoice PDFs** with IBAN color coding
- **File naming pattern:** `invoice_XXX_color_B_YYY.pdf`
- **Total size:** ~45MB (45KB per file average)

### **Destination:** Snowflake Stage
- **Stage name:** `@DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE`
- **Access:** Available to all your enhanced AI applications
- **Processing:** Automatic Document AI extraction via tasks

## ⏱️ **Upload Timeline**

| File Count | Estimated Time | Progress Updates |
|------------|----------------|------------------|
| 1000 files | 10-15 minutes  | Every 100 files |
| 500 files  | 5-8 minutes    | Every 100 files |
| 100 files  | 1-2 minutes    | Every 50 files  |

**Factors affecting speed:**
- Internet connection bandwidth
- Snowflake region proximity  
- File sizes and formats

## 🔧 **Troubleshooting**

### **Common Issues & Solutions**

#### **1. SnowCLI Not Found**
```bash
❌ Error: snowcli not found!
```
**Solution:**
```bash
pip install snowflake-cli-labs
# or: pipx install snowflake-cli-labs
```

#### **2. Connection Failed**
```bash
❌ Error: SnowCLI connection failed!
```
**Solutions:**
```bash
# Check existing connections
snow connection list

# Test specific connection
snow connection test

# Add new connection
snow connection add
```

#### **3. Stage Not Accessible**
```bash
❌ Stage @DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE not accessible
```
**Solutions:**
```bash
# Run setup script
snow sql --filename docai_invoice_qs_setup.sql

# Check role permissions
snow sql --query "SHOW GRANTS TO ROLE DOC_AI_QS_ROLE"

# Verify database access
snow sql --query "USE DATABASE DOC_AI_QS_DB"
```

#### **4. Permission Denied**
```bash
❌ Access denied for stage operations
```
**Solutions:**
```bash
# Switch to correct role
snow sql --query "USE ROLE DOC_AI_QS_ROLE"

# Check current role
snow sql --query "SELECT CURRENT_ROLE()"

# Verify stage permissions
snow sql --query "SHOW STAGES IN DOC_AI_QS_DB.DOC_AI_SCHEMA"
```

#### **5. Dataset Directory Not Found**
```bash
❌ Dataset_colored_IBAN directory not found!
```
**Solutions:**
- Ensure you're in the project root directory
- Verify the dataset directory exists
- Check directory permissions

## 🎯 **Post-Upload Steps**

### **1. Verify Upload Success**
```bash
# List files in stage
snow stage list @DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE

# Count uploaded files
snow sql --query "
SELECT COUNT(*) as uploaded_files 
FROM DIRECTORY(@DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE)
"
```

### **2. Initialize AI Processing**
```bash
# Set up enhanced AI features
snow sql --filename cortex_enhancements.sql

# Run initial fraud assessment
snow sql --query "CALL SP_BATCH_FRAUD_ASSESSMENT()"

# Run AI categorization
snow sql --query "CALL SP_AI_CATEGORIZE_INVOICES()"

# Detect anomalies
snow sql --query "CALL SP_DETECT_ANOMALIES()"
```

### **3. Test Your Applications**

#### **Streamlit in Snowflake (Recommended)**
1. Open Snowsight in your browser
2. Go to **Streamlit** → **Create App**
3. Copy content from `sis_basic_invoice_app.py`
4. Deploy and test with your new dataset

#### **Local Applications**
```bash
# Basic SiS-compatible app
streamlit run sis_basic_invoice_app.py

# Enhanced local app (if conda environment works)
conda activate invoice-ai-env
streamlit run cortex_enhanced_app.py

# Upload interface
streamlit run streamlit_bulk_upload.py
```

## 🧠 **AI Features Available After Upload**

### **🚨 Fraud Detection**
- **Statistical Analysis:** Z-score anomaly detection
- **AI Assessment:** Cortex AI risk evaluation
- **Pattern Recognition:** Unusual invoice patterns

### **🏷️ Smart Categorization**
- **AI Classification:** Automatic invoice type detection
- **Category Insights:** Spending pattern analysis
- **Budget Tracking:** Category-based reporting

### **💬 AI Assistant**
- **Natural Language Queries:** Ask about your data
- **Conversational Analysis:** Chat with your invoices
- **Insights Generation:** AI-powered recommendations

### **📊 Advanced Analytics**
- **Trend Analysis:** Spending patterns over time
- **Anomaly Detection:** Statistical outliers
- **Predictive Insights:** Future spending forecasts

## 🔗 **Next Steps**

### **For Development:**
1. **Explore the data** with enhanced Streamlit apps
2. **Customize AI models** for your specific use cases
3. **Extend analytics** with additional business logic

### **For Production:**
1. **Deploy to Streamlit in Snowflake** for enterprise use
2. **Set up automated monitoring** with AI alerts
3. **Integrate with business systems** via APIs

### **For Demo/Testing:**
1. **Use the demo test script:** `snow sql --filename demo_test_script.sql`
2. **Show AI capabilities** to stakeholders
3. **Demonstrate ROI** with automated processing

## 📞 **Support Resources**

- **SnowCLI Documentation:** [docs.snowflake.com/developer-guide/snowflake-cli](https://docs.snowflake.com/en/developer-guide/snowflake-cli)
- **Snowflake Cortex:** [docs.snowflake.com/cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- **Document AI:** [docs.snowflake.com/document-ai](https://docs.snowflake.com/en/user-guide/snowflake-document-ai)

---

## 📝 **Summary Commands**

```bash
# Quick setup verification
./verify_snowcli_setup.sh

# Upload your dataset  
./snowcli_upload_simple.sh

# Initialize AI features
snow sql --filename cortex_enhancements.sql

# Test with enhanced apps
streamlit run sis_basic_invoice_app.py
```

**🎉 You're now ready to analyze 1000+ invoices with AI-powered intelligence!** 