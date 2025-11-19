#!/bin/bash

# =================================================================
# Bulk Upload Script for Dataset_colored_IBAN
# =================================================================

echo "🚀 Starting bulk upload of Dataset_colored_IBAN to Snowflake..."
echo "=================================================================="

# Check if we're in the right directory
if [ ! -d "Dataset_colored_IBAN" ]; then
    echo "❌ Error: Dataset_colored_IBAN directory not found!"
    echo "   Make sure you're running this from the project root directory"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Check if Python script exists
if [ ! -f "bulk_upload_dataset.py" ]; then
    echo "❌ Error: bulk_upload_dataset.py not found!"
    echo "   Make sure the upload script is in the current directory"
    exit 1
fi

# Activate conda environment if it exists
if command -v conda &> /dev/null; then
    echo "🔍 Checking for conda environment..."
    if conda env list | grep -q "invoice-ai-env"; then
        echo "✅ Activating conda environment: invoice-ai-env"
        eval "$(conda shell.bash hook)"
        conda activate invoice-ai-env
    else
        echo "⚠️  Conda environment 'invoice-ai-env' not found, using system Python"
    fi
else
    echo "⚠️  Conda not found, using system Python"
fi

# Check Python dependencies
echo "🔍 Checking Python dependencies..."
python -c "import snowflake.snowpark; print('✅ Snowpark available')" 2>/dev/null || {
    echo "❌ Error: snowflake-snowpark-python not available"
    echo "   Please install it with: pip install snowflake-snowpark-python"
    exit 1
}

# Run the upload script
echo ""
echo "🚀 Starting bulk upload process..."
echo "   This may take several minutes for 1000+ files..."
echo ""

python bulk_upload_dataset.py

upload_exit_code=$?

echo ""
echo "=================================================================="

if [ $upload_exit_code -eq 0 ]; then
    echo "🎉 Bulk upload completed successfully!"
    echo ""
    echo "🔗 Next Steps:"
    echo "   1. Run Streamlit apps to use your enhanced dataset:"
    echo "      streamlit run sis_basic_invoice_app.py"
    echo "      streamlit run streamlit_bulk_upload.py"
    echo "      streamlit run cortex_enhanced_app.py"
    echo ""
    echo "   2. Deploy to Streamlit in Snowflake for best experience"
    echo "   3. Test AI features with your 1000+ invoice dataset"
else
    echo "❌ Upload completed with errors (exit code: $upload_exit_code)"
    echo "   Check the output above for details"
    echo "   You can re-run this script to resume uploading"
fi

echo "==================================================================" 