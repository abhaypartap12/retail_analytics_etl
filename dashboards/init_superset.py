import os
import uuid
from superset.app import create_app

# Create the Flask app
app = create_app()

with app.app_context():
    print("✅ Flask app context loaded.")

    # Now safe to import context-bound modules
    from superset import db
    from superset.models.core import Database

    # Step 1: Register a new DB connection with fixed UUID
    db_name = "PostgreSQL"
    conn_str = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    db_uuid = uuid.UUID("8ff2ba3b-8876-4bbf-89ad-d7dea1dcbb52")

    existing_db = db.session.query(Database).filter_by(database_name=db_name).first()
    if existing_db:
        print("ℹ️ Database already exists.")
        if str(existing_db.uuid) != str(db_uuid):
            print("🔁 Updating existing DB to use fixed UUID...")
            existing_db.uuid = db_uuid
            db.session.commit()
            print("✅ UUID updated.")
    else:
        print("🔗 Creating new database connection with fixed UUID...")
        new_db = Database(
            uuid=db_uuid,
            database_name=db_name,
            sqlalchemy_uri=conn_str,
            expose_in_sqllab=True,
            extra='{"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}, "schemas_allowed_for_csv_upload": []}'
        )
        db.session.add(new_db)
        db.session.commit()
        print("✅ Database registered.")

    # Step 2: Import datasets if folder exists
    dataset_path = "/app/dashboards/datasets"
    if os.path.exists(dataset_path):
        print("📊 Importing datasets...")
        os.system(f"superset import-datasources --path {dataset_path}")
    else:
        print("⚠️ Dataset folder not found, skipping dataset import.")

    # Step 3: Import dashboards if ZIP exists
    zip_path = "/app/dashboards/dashboard.zip"
    if os.path.exists(zip_path):
        print("📦 Importing dashboards...")
        result = os.system(f"superset import-dashboards --path {zip_path} --username admin")
        if result != 0:
            print("❌ Failed to import dashboards. Check Superset logs.")
        else:
            print("✅ Dashboards imported successfully.")
    else:
        print("⚠️ Dashboard zip not found, skipping import.")
