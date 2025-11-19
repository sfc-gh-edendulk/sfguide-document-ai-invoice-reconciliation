import streamlit as st
import os
import io
import time
from pathlib import Path
from snowflake.snowpark.context import get_active_session
import pandas as pd

st.set_page_config(
    page_title="📤 Bulk Dataset Upload", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

# Source directory
DATASET_DIR = "/Users/edendulk/code/sfguide-document-ai-invoice-reconciliation/Dataset_colored_IBAN"

st.title("📤 Bulk Dataset Upload to Snowflake")
st.markdown("Upload all 1000+ invoice PDFs from Dataset_colored_IBAN to the Snowflake stage")

# Initialize session state
if 'upload_progress' not in st.session_state:
    st.session_state.upload_progress = 0
if 'upload_status' not in st.session_state:
    st.session_state.upload_status = "ready"
if 'upload_results' not in st.session_state:
    st.session_state.upload_results = None

# Get Snowflake session
try:
    session = get_active_session()
    st.success("❄️ Snowflake session established!")
except Exception as e:
    st.error(f"❌ Error getting Snowflake session: {e}")
    st.stop()

@st.cache_data
def get_dataset_info():
    """Get information about the dataset directory"""
    if not os.path.exists(DATASET_DIR):
        return None, []
    
    pdf_files = list(Path(DATASET_DIR).glob("*.pdf"))
    return len(pdf_files), [f.name for f in pdf_files[:10]]  # Show first 10 for preview

@st.cache_data
def get_stage_files():
    """Get list of files currently in the Snowflake stage"""
    try:
        result = session.sql(f"LIST @{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}").collect()
        stage_files = [row['name'].split('/')[-1] for row in result]
        return len(stage_files), stage_files[:10]  # Show first 10 for preview
    except Exception as e:
        st.error(f"Error accessing stage: {e}")
        return 0, []

def upload_file_to_stage(file_path, stage_file_name):
    """Upload a single file to the Snowflake stage"""
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
        
        session.file.put_stream(
            io.BytesIO(file_bytes), 
            f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{stage_file_name}", 
            overwrite=True, 
            auto_compress=False
        )
        return True
    except Exception as e:
        st.error(f"Error uploading {stage_file_name}: {e}")
        return False

# Sidebar with information
with st.sidebar:
    st.header("📊 Upload Status")
    
    # Dataset information
    local_count, sample_files = get_dataset_info()
    if local_count:
        st.success(f"📁 Found {local_count} PDF files")
        st.write("Sample files:")
        for i, file_name in enumerate(sample_files, 1):
            st.write(f"  {i}. {file_name}")
        if local_count > 10:
            st.write(f"  ... and {local_count - 10} more")
    else:
        st.error("❌ Dataset directory not found!")
        st.stop()
    
    st.divider()
    
    # Stage information  
    stage_count, stage_sample = get_stage_files()
    st.info(f"☁️ Stage has {stage_count} files")
    
    if stage_count > 0:
        st.write("Recent stage files:")
        for i, file_name in enumerate(stage_sample, 1):
            st.write(f"  {i}. {file_name}")
        if stage_count > 10:
            st.write(f"  ... and {stage_count - 10} more")

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.header("🚀 Bulk Upload Controls")
    
    # Upload statistics
    files_to_upload = local_count - stage_count if local_count > stage_count else 0
    
    if files_to_upload == 0:
        st.success("✅ All files already uploaded!")
        st.balloons()
    else:
        st.warning(f"📤 {files_to_upload} files need to be uploaded")
    
    # Upload options
    batch_size = st.slider("Batch size (files per batch):", 1, 100, 25)
    st.write(f"Upload will process {batch_size} files at a time")
    
    # Upload controls
    col1a, col1b, col1c = st.columns(3)
    
    with col1a:
        if st.button("🚀 Start Bulk Upload", type="primary", disabled=(files_to_upload == 0)):
            st.session_state.upload_status = "uploading"
            st.rerun()
    
    with col1b:
        if st.button("🔄 Refresh Status"):
            st.cache_data.clear()
            st.rerun()
    
    with col1c:
        if st.button("🧹 Clear Upload Cache"):
            st.cache_data.clear()
            st.session_state.upload_results = None
            st.rerun()

with col2:
    st.header("📈 Progress")
    
    if st.session_state.upload_status == "uploading":
        st.info("Upload in progress...")
        progress_bar = st.progress(0)
        status_text = st.empty()
    elif st.session_state.upload_results:
        results = st.session_state.upload_results
        st.metric("✅ Successful", results['success'])
        st.metric("❌ Failed", results['failed'])
        st.metric("📁 Total in Stage", results['total_stage'])

# Bulk upload logic
if st.session_state.upload_status == "uploading":
    st.header("🔄 Upload in Progress")
    
    # Get files to upload
    pdf_files = list(Path(DATASET_DIR).glob("*.pdf"))
    
    # Get already uploaded files
    try:
        result = session.sql(f"LIST @{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}").collect()
        uploaded_files = set(row['name'].split('/')[-1] for row in result)
    except:
        uploaded_files = set()
    
    files_to_upload = [f for f in pdf_files if f.name not in uploaded_files]
    
    if len(files_to_upload) == 0:
        st.success("✅ All files already uploaded!")
        st.session_state.upload_status = "completed"
        st.rerun()
    
    # Create progress tracking
    total_files = len(files_to_upload)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    success_count = 0
    error_count = 0
    
    # Upload in batches
    for i, file_path in enumerate(files_to_upload):
        file_name = file_path.name
        
        # Update progress
        progress = (i + 1) / total_files
        progress_bar.progress(progress)
        status_text.text(f"Uploading {i+1}/{total_files}: {file_name}")
        
        # Upload file
        if upload_file_to_stage(file_path, file_name):
            success_count += 1
        else:
            error_count += 1
        
        # Batch refresh every 25 files
        if (i + 1) % batch_size == 0:
            try:
                session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
                status_text.text(f"Refreshed stage after {i+1} files...")
                time.sleep(0.5)  # Brief pause
            except:
                pass
    
    # Final refresh
    try:
        session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
    except:
        pass
    
    # Store results
    st.session_state.upload_results = {
        'success': success_count,
        'failed': error_count,
        'total_stage': success_count + len(uploaded_files)
    }
    
    # Update status
    st.session_state.upload_status = "completed"
    
    # Show completion
    st.success(f"🎉 Upload completed! {success_count} files uploaded successfully")
    if error_count > 0:
        st.error(f"❌ {error_count} files failed to upload")
    
    st.balloons()
    time.sleep(2)
    st.rerun()

# Results section
if st.session_state.upload_results:
    st.header("📊 Upload Results")
    results = st.session_state.upload_results
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ Successfully Uploaded", results['success'])
    with col2:
        st.metric("❌ Failed Uploads", results['failed'])
    with col3:
        st.metric("📁 Total Files in Stage", results['total_stage'])
    
    # Verification
    st.subheader("🔍 Verification")
    verification_col1, verification_col2 = st.columns(2)
    
    with verification_col1:
        st.write("**Local Dataset:**")
        local_count, _ = get_dataset_info()
        st.write(f"📁 {local_count} PDF files")
    
    with verification_col2:
        st.write("**Snowflake Stage:**")
        stage_count, _ = get_stage_files()
        st.write(f"☁️ {stage_count} files")
    
    if stage_count >= local_count:
        st.success("✅ Upload verification successful! All files are in the stage.")
    else:
        st.warning(f"⚠️ {local_count - stage_count} files may be missing from stage")

# Next steps
st.header("🔗 Next Steps")
st.markdown("""
After your bulk upload is complete, you can use your enhanced AI applications:

### 🚀 **Streamlit in Snowflake (Recommended)**
- Deploy `sis_basic_invoice_app.py` or `sis_enhanced_invoice_app.py` to SiS
- No environment issues, runs directly in Snowflake
- Access via Snowsight → Streamlit

### 💻 **Local Applications**
```bash
# Activate your conda environment
conda activate invoice-ai-env

# Run enhanced applications
streamlit run cortex_enhanced_app.py
streamlit run enhanced_invoice_app.py
```

### 🧠 **AI Features Now Available**
- **Fraud Detection**: Analyze 1000+ invoices for anomalies
- **AI Categorization**: Smart classification of invoice types  
- **AI Assistant**: Chat about your invoice data
- **Advanced Analytics**: Patterns and insights from large dataset
""")

# Footer
st.divider()
st.markdown("**🚀 Powered by Snowflake + Document AI**") 