#!/bin/bash

# =================================================================
# Simple Bulk Upload using SnowCLI (no external dependencies)
# =================================================================

echo "🚀 Simple bulk upload of Dataset_colored_IBAN using SnowCLI..."
echo "=================================================================="

# Configuration
DATASET_DIR="Dataset_colored_IBAN"
STAGE_NAME="@DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_STAGE"

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check directory
if [ ! -d "$DATASET_DIR" ]; then
    echo "❌ Error: $DATASET_DIR directory not found!"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Check snowcli
if ! command -v snow &> /dev/null; then
    echo "❌ Error: snowcli not found!"
    echo "   Install with: pip install snowflake-cli-labs"
    exit 1
fi

# Test connection
echo "🔍 Testing SnowCLI connection..."
if ! snow sql --query "SELECT CURRENT_USER()" --quiet > /dev/null 2>&1; then
    echo "❌ Error: SnowCLI connection failed!"
    echo "   Configure with: snow connection add"
    exit 1
fi
echo "✅ SnowCLI connection working"

# Count files
total_files=$(find "$DATASET_DIR" -name "*.pdf" | wc -l | tr -d ' ')
echo "📁 Found $total_files PDF files"

if [ "$total_files" -eq 0 ]; then
    echo "❌ No PDF files found"
    exit 1
fi

# Start upload
echo ""
echo "🚀 Starting upload (this may take 10-20 minutes for 1000+ files)..."
echo ""

uploaded=0
failed=0

# Upload each file
for pdf_file in "$DATASET_DIR"/*.pdf; do
    if [ -f "$pdf_file" ]; then
        filename=$(basename "$pdf_file")
        
        # Show progress every 100 files
        if [ $((uploaded % 100)) -eq 0 ]; then
            echo "📤 Progress: $uploaded/$total_files files uploaded..."
        fi
        
        # Upload file
        if snow stage put "$pdf_file" "$STAGE_NAME" --overwrite --quiet > /dev/null 2>&1; then
            ((uploaded++))
            echo -n "."
        else
            ((failed++))
            echo -n "❌"
            echo " Failed: $filename" >&2
        fi
        
        # Refresh stage every 200 files
        if [ $((uploaded % 200)) -eq 0 ] && [ $uploaded -gt 0 ]; then
            echo ""
            echo "🔄 Refreshing stage..."
            snow sql --query "ALTER STAGE $STAGE_NAME REFRESH" --quiet > /dev/null 2>&1
        fi
    fi
done

echo ""
echo ""

# Final stage refresh
echo "🔄 Final stage refresh..."
snow sql --query "ALTER STAGE $STAGE_NAME REFRESH" --quiet

# Results
echo ""
echo "📊 Upload Results:"
echo "   ✅ Uploaded: $uploaded files"
echo "   ❌ Failed: $failed files"
echo "   📁 Total: $((uploaded + failed)) files processed"

# Simple verification
echo ""
echo "🔍 Quick verification..."
stage_list_output=$(snow stage list "$STAGE_NAME" 2>/dev/null)
if [ $? -eq 0 ]; then
    echo "✅ Stage access confirmed"
    echo "   (Run 'snow stage list $STAGE_NAME' to see all files)"
else
    echo "⚠️  Could not verify stage contents"
fi

echo ""
echo "=================================================================="

if [ $failed -eq 0 ]; then
    echo "🎉 SUCCESS! All $uploaded files uploaded"
    echo ""
    echo "🔗 Next Steps:"
    echo "   1. Test your apps with the new dataset:"
    echo "      streamlit run sis_basic_invoice_app.py"
    echo ""
    echo "   2. Initialize AI processing:"
    echo "      snow sql --filename cortex_enhancements.sql"
    echo ""
    echo "   3. Run initial AI analysis:"
    echo "      snow sql --query \"CALL SP_BATCH_FRAUD_ASSESSMENT()\""
else
    echo "⚠️  Completed with $failed errors"
    echo "   Re-run this script to retry failed uploads"
fi

echo "==================================================================" 