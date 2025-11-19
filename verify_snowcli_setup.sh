#!/bin/bash

# =================================================================
# SnowCLI Setup Verification Script
# =================================================================

echo "🔍 Verifying SnowCLI setup for bulk upload..."
echo "================================================"

# Configuration
STAGE_NAME="@DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE"
DB_NAME="DOC_AI_QS_DB"
SCHEMA_NAME="DOC_AI_SCHEMA"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check 1: SnowCLI Installation
echo "1️⃣  Checking SnowCLI installation..."
if command -v snow &> /dev/null; then
    snow_version=$(snow --version 2>/dev/null || echo "unknown")
    echo -e "   ${GREEN}✅ SnowCLI found: $snow_version${NC}"
else
    echo -e "   ${RED}❌ SnowCLI not found${NC}"
    echo "   Install with: pip install snowflake-cli-labs"
    exit 1
fi

# Check 2: Connection Configuration
echo ""
echo "2️⃣  Checking SnowCLI connection..."
if snow connection list > /dev/null 2>&1; then
    echo -e "   ${GREEN}✅ SnowCLI connections configured${NC}"
    echo "   Available connections:"
    snow connection list | grep -E "^\s*\*?\s*[a-zA-Z]" | sed 's/^/      /'
else
    echo -e "   ${RED}❌ No SnowCLI connections found${NC}"
    echo "   Configure with: snow connection add"
    exit 1
fi

# Check 3: Database Connection
echo ""
echo "3️⃣  Testing database connection..."
current_user=$(snow sql --query "SELECT CURRENT_USER()" --format plain 2>/dev/null | tail -1)
if [ $? -eq 0 ] && [ ! -z "$current_user" ]; then
    echo -e "   ${GREEN}✅ Database connection successful${NC}"
    echo "   Connected as: $current_user"
else
    echo -e "   ${RED}❌ Database connection failed${NC}"
    echo "   Check your connection with: snow connection test"
    exit 1
fi

# Check 4: Database and Schema Access
echo ""
echo "4️⃣  Checking database and schema access..."

# Check database
db_exists=$(snow sql --query "SHOW DATABASES LIKE '$DB_NAME'" --format plain 2>/dev/null | tail -1)
if echo "$db_exists" | grep -q "$DB_NAME"; then
    echo -e "   ${GREEN}✅ Database $DB_NAME accessible${NC}"
else
    echo -e "   ${YELLOW}⚠️  Database $DB_NAME not found or not accessible${NC}"
    echo "   You may need to run the setup script first"
fi

# Check schema
schema_exists=$(snow sql --query "SHOW SCHEMAS IN DATABASE $DB_NAME LIKE '$SCHEMA_NAME'" --format plain 2>/dev/null | tail -1)
if echo "$schema_exists" | grep -q "$SCHEMA_NAME"; then
    echo -e "   ${GREEN}✅ Schema $SCHEMA_NAME accessible${NC}"
else
    echo -e "   ${YELLOW}⚠️  Schema $SCHEMA_NAME not found or not accessible${NC}"
    echo "   You may need to run the setup script first"
fi

# Check 5: Stage Access
echo ""
echo "5️⃣  Checking stage access..."
stage_list=$(snow stage list "$STAGE_NAME" 2>/dev/null)
if [ $? -eq 0 ]; then
    file_count=$(echo "$stage_list" | wc -l | tr -d ' ')
    echo -e "   ${GREEN}✅ Stage $STAGE_NAME accessible${NC}"
    echo "   Current files in stage: $((file_count - 1))"  # Subtract header
else
    echo -e "   ${RED}❌ Stage $STAGE_NAME not accessible${NC}"
    echo "   You may need to:"
    echo "   - Run docai_invoice_qs_setup.sql"
    echo "   - Check your role permissions"
    exit 1
fi

# Check 6: Dataset Directory
echo ""
echo "6️⃣  Checking local dataset..."
if [ -d "Dataset_colored_IBAN" ]; then
    pdf_count=$(find Dataset_colored_IBAN -name "*.pdf" | wc -l | tr -d ' ')
    echo -e "   ${GREEN}✅ Dataset directory found${NC}"
    echo "   PDF files ready for upload: $pdf_count"
    
    if [ "$pdf_count" -eq 0 ]; then
        echo -e "   ${YELLOW}⚠️  No PDF files found in dataset directory${NC}"
    fi
else
    echo -e "   ${RED}❌ Dataset_colored_IBAN directory not found${NC}"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Check 7: Cortex AI Access (Optional)
echo ""
echo "7️⃣  Checking Cortex AI access (optional)..."
cortex_test=$(snow sql --query "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', 'test')" --format plain 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✅ Cortex AI accessible${NC}"
    echo "   AI features will be available after upload"
else
    echo -e "   ${YELLOW}⚠️  Cortex AI not accessible${NC}"
    echo "   Basic features will work, but AI features may be limited"
    echo "   Contact your admin to enable Cortex AI"
fi

# Summary
echo ""
echo "================================================"
echo "🎯 Setup Verification Summary"
echo "================================================"

if [ -d "Dataset_colored_IBAN" ] && [ "$pdf_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Ready for bulk upload!${NC}"
    echo ""
    echo "📤 To start upload, run:"
    echo "   ./snowcli_upload_simple.sh"
    echo ""
    echo "📊 Your dataset:"
    echo "   - $pdf_count PDF files ready"
    echo "   - Estimated upload time: 10-20 minutes"
    echo ""
    echo "🚀 After upload, you can:"
    echo "   1. Run enhanced Streamlit apps"
    echo "   2. Deploy to Streamlit in Snowflake"
    echo "   3. Use AI features for analysis"
else
    echo -e "${YELLOW}⚠️  Setup has issues, check output above${NC}"
fi

echo "================================================" 