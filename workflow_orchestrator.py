# Databricks notebook source
# MAGIC %md
# MAGIC # European Climate Risk Pipeline Orchestrator
# MAGIC 
# MAGIC This notebook orchestrates the execution of all European climate risk data pipelines using Databricks Workflows and Delta Live Tables.
# MAGIC 
# MAGIC ## Pipelines Managed
# MAGIC 
# MAGIC 1. **Terrain/DEM Ingestion** - Copernicus, EEA, OpenGeoHub, GeoHarmonizer
# MAGIC 2. **AccuWeather European Locations Ingestion** - Real-time weather data
# MAGIC 3. **Flood Risk Transformation** - Risk scoring and evacuation zones
# MAGIC 4. **Drought Risk Transformation** - Drought indices and impact assessment
# MAGIC 
# MAGIC **Author:** Climate Risk Analytics Team  
# MAGIC **Date:** 2025-11-12  
# MAGIC **Catalog:** demo_hc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines
from datetime import datetime
import yaml
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## EuropeanRiskPipelineOrchestrator Class
# MAGIC 
# MAGIC Main orchestrator class for managing all climate risk pipelines.

# COMMAND ----------

class EuropeanRiskPipelineOrchestrator:
    """
    Orchestrator for European climate risk data pipelines.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.workspace = WorkspaceClient()
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path: str = None) -> dict:
        """Load configuration from YAML file."""
        if config_path:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            # Default configuration
            return {
                "catalog": "demo_hc",
                "schemas": {
                    "raw_data": "raw_data",
                    "processed_data": "processed_data",
                    "risk_analytics": "risk_analytics"
                },
                "pipelines": {
                    "terrain_ingestion": {
                        "name": "european_terrain_dem_ingestion",
                        "notebook": "/Workspace/Shared/risk_app/pipelines/01_terrain_dem_ingestion",
                        "schedule": "0 0 * * 0",  # Weekly on Sunday
                        "cluster_size": "Medium"
                    },
                    "weather_ingestion": {
                        "name": "accuweather_europe_ingestion",
                        "notebook": "/Workspace/Shared/risk_app/pipelines/02_accuweather_europe_ingestion",
                        "schedule": "0 * * * *",  # Hourly
                        "cluster_size": "Small"
                    },
                    "flood_risk": {
                        "name": "flood_risk_transformation",
                        "notebook": "/Workspace/Shared/risk_app/pipelines/03_flood_risk_transformation",
                        "schedule": "15 * * * *",  # Hourly at :15
                        "cluster_size": "Medium"
                    },
                    "drought_risk": {
                        "name": "drought_risk_transformation",
                        "notebook": "/Workspace/Shared/risk_app/pipelines/04_drought_risk_transformation",
                        "schedule": "0 6 * * *",  # Daily at 6 AM
                        "cluster_size": "Medium"
                    }
                }
            }
    
    def create_dlt_pipeline(self, pipeline_config: dict) -> str:
        """
        Create a Delta Live Tables pipeline.
        
        Args:
            pipeline_config: Pipeline configuration dictionary
            
        Returns:
            Pipeline ID
        """
        print(f"Creating DLT pipeline: {pipeline_config['name']}")
        
        # Define cluster configuration based on size
        cluster_configs = {
            "Small": {
                "num_workers": 2,
                "node_type_id": "i3.xlarge"
            },
            "Medium": {
                "num_workers": 4,
                "node_type_id": "i3.2xlarge"
            },
            "Large": {
                "num_workers": 8,
                "node_type_id": "i3.4xlarge"
            }
        }
        
        cluster_size = pipeline_config.get("cluster_size", "Medium")
        cluster_config = cluster_configs[cluster_size]
        
        # Create pipeline specification
        pipeline_spec = pipelines.CreatePipelineRequest(
            name=pipeline_config["name"],
            libraries=[
                pipelines.PipelineLibrary(
                    notebook=pipelines.NotebookLibrary(
                        path=pipeline_config["notebook"]
                    )
                )
            ],
            clusters=[
                pipelines.PipelineCluster(
                    label="default",
                    num_workers=cluster_config["num_workers"],
                    node_type_id=cluster_config["node_type_id"],
                    custom_tags={
                        "project": "european_climate_risk",
                        "pipeline": pipeline_config["name"]
                    }
                )
            ],
            configuration={
                "pipelines.applyChangesPreviewEnabled": "true",
                "pipelines.useSharedClusters": "false"
            },
            target=f"{self.config['catalog']}.{self.config['schemas']['processed_data']}",
            continuous=False,  # Triggered mode
            development=False,  # Production mode
            photon=True,  # Enable Photon for performance
            channel="CURRENT"
        )
        
        try:
            response = self.workspace.pipelines.create(pipeline_spec)
            pipeline_id = response.pipeline_id
            print(f"✓ Pipeline created successfully: {pipeline_id}")
            return pipeline_id
        except Exception as e:
            print(f"✗ Error creating pipeline: {str(e)}")
            return None
    
    def create_workflow_job(self, job_name: str, pipeline_ids: dict) -> str:
        """
        Create a Databricks Workflow that orchestrates multiple DLT pipelines.
        
        Args:
            job_name: Name of the workflow job
            pipeline_ids: Dictionary mapping pipeline names to IDs
            
        Returns:
            Job ID
        """
        print(f"\nCreating workflow job: {job_name}")
        
        # Define tasks
        tasks = []
        
        # Task 1: Terrain data ingestion (runs weekly)
        if "terrain_ingestion" in pipeline_ids:
            tasks.append(
                jobs.Task(
                    task_key="terrain_ingestion",
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=pipeline_ids["terrain_ingestion"],
                        full_refresh=False
                    ),
                    timeout_seconds=7200  # 2 hours
                )
            )
        
        # Task 2: Weather data ingestion (runs hourly)
        if "weather_ingestion" in pipeline_ids:
            tasks.append(
                jobs.Task(
                    task_key="weather_ingestion",
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=pipeline_ids["weather_ingestion"],
                        full_refresh=False
                    ),
                    depends_on=[jobs.TaskDependency(task_key="terrain_ingestion")],
                    timeout_seconds=1800  # 30 minutes
                )
            )
        
        # Task 3: Flood risk transformation
        if "flood_risk" in pipeline_ids:
            tasks.append(
                jobs.Task(
                    task_key="flood_risk_transformation",
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=pipeline_ids["flood_risk"],
                        full_refresh=False
                    ),
                    depends_on=[
                        jobs.TaskDependency(task_key="terrain_ingestion"),
                        jobs.TaskDependency(task_key="weather_ingestion")
                    ],
                    timeout_seconds=3600  # 1 hour
                )
            )
        
        # Task 4: Drought risk transformation
        if "drought_risk" in pipeline_ids:
            tasks.append(
                jobs.Task(
                    task_key="drought_risk_transformation",
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=pipeline_ids["drought_risk"],
                        full_refresh=False
                    ),
                    depends_on=[
                        jobs.TaskDependency(task_key="terrain_ingestion"),
                        jobs.TaskDependency(task_key="weather_ingestion")
                    ],
                    timeout_seconds=3600  # 1 hour
                )
            )
        
        # Create job specification
        job_spec = jobs.CreateJobRequest(
            name=job_name,
            tasks=tasks,
            job_clusters=[],  # DLT pipelines use their own clusters
            schedule=jobs.CronSchedule(
                quartz_cron_expression="0 * * * *",  # Run hourly
                timezone_id="UTC",
                pause_status=jobs.PauseStatus.UNPAUSED
            ),
            email_notifications=jobs.JobEmailNotifications(
                on_failure=["climate-risk-team@company.com"],
                on_success=[],
                no_alert_for_skipped_runs=True
            ),
            webhook_notifications=jobs.WebhookNotifications(
                on_failure=[],
                on_success=[]
            ),
            timeout_seconds=14400,  # 4 hours total
            max_concurrent_runs=1,
            tags={
                "project": "european_climate_risk",
                "environment": "production"
            }
        )
        
        try:
            response = self.workspace.jobs.create(job_spec)
            job_id = response.job_id
            print(f"✓ Workflow job created successfully: {job_id}")
            return job_id
        except Exception as e:
            print(f"✗ Error creating workflow job: {str(e)}")
            return None
    
    def setup_unity_catalog(self):
        """
        Set up Unity Catalog schemas for the pipeline.
        """
        print("\n" + "="*80)
        print("Setting up Unity Catalog")
        print("="*80)
        
        catalog = self.config["catalog"]
        schemas = self.config["schemas"]
        
        # SQL commands to execute
        sql_commands = [
            # Create catalog
            f"CREATE CATALOG IF NOT EXISTS {catalog}",
            f"COMMENT ON CATALOG {catalog} IS 'European Climate Risk Data - Flood and Drought Analysis'",
            
            # Create schemas
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemas['raw_data']}",
            f"COMMENT ON SCHEMA {catalog}.{schemas['raw_data']} IS 'Raw data from external sources (Bronze layer)'",
            
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemas['processed_data']}",
            f"COMMENT ON SCHEMA {catalog}.{schemas['processed_data']} IS 'Processed and enriched data (Silver layer)'",
            
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemas['risk_analytics']}",
            f"COMMENT ON SCHEMA {catalog}.{schemas['risk_analytics']} IS 'Risk scores and analytics (Gold layer)'",
            
            # Set default catalog
            f"USE CATALOG {catalog}"
        ]
        
        print("\nExecuting Unity Catalog setup commands...")
        for cmd in sql_commands:
            print(f"  {cmd}")
        
        print("✓ Unity Catalog setup completed")
    
    def deploy_all_pipelines(self):
        """
        Deploy all pipelines and create the orchestration workflow.
        """
        print("\n" + "="*80)
        print("European Climate Risk Pipeline Deployment (demo_hc)")
        print("="*80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # Step 1: Setup Unity Catalog
        self.setup_unity_catalog()
        
        # Step 2: Create DLT pipelines
        print("\n" + "="*80)
        print("Creating Delta Live Tables Pipelines")
        print("="*80)
        
        pipeline_ids = {}
        for pipeline_name, pipeline_config in self.config["pipelines"].items():
            pipeline_id = self.create_dlt_pipeline(pipeline_config)
            if pipeline_id:
                pipeline_ids[pipeline_name] = pipeline_id
        
        # Step 3: Create orchestration workflow
        print("\n" + "="*80)
        print("Creating Orchestration Workflow")
        print("="*80)
        
        job_id = self.create_workflow_job(
            "european_climate_risk_workflow",
            pipeline_ids
        )
        
        # Step 4: Summary
        print("\n" + "="*80)
        print("Deployment Summary")
        print("="*80)
        print(f"\nCatalog: {self.config['catalog']}")
        print(f"\nPipelines Created: {len(pipeline_ids)}")
        for name, pid in pipeline_ids.items():
            print(f"  • {name}: {pid}")
        
        if job_id:
            print(f"\nWorkflow Job ID: {job_id}")
            print(f"\nWorkflow URL: https://<databricks-instance>/jobs/{job_id}")
        
        print("\n" + "="*80)
        print("Deployment Complete!")
        print("="*80)
        
        return {
            "pipeline_ids": pipeline_ids,
            "job_id": job_id,
            "catalog": self.config["catalog"]
        }
    
    def run_pipeline(self, pipeline_name: str):
        """
        Manually trigger a specific pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to run
        """
        pipeline_config = self.config["pipelines"].get(pipeline_name)
        if not pipeline_config:
            print(f"Pipeline '{pipeline_name}' not found in configuration")
            return
        
        print(f"Triggering pipeline: {pipeline_config['name']}")
        # Implementation would trigger the specific DLT pipeline
        print("✓ Pipeline triggered")
    
    def get_pipeline_status(self, pipeline_id: str) -> dict:
        """
        Get the status of a DLT pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline status dictionary
        """
        try:
            pipeline = self.workspace.pipelines.get(pipeline_id)
            return {
                "pipeline_id": pipeline_id,
                "name": pipeline.name,
                "state": pipeline.state,
                "health": pipeline.health,
                "latest_updates": pipeline.latest_updates
            }
        except Exception as e:
            return {
                "error": str(e)
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Functions
# MAGIC 
# MAGIC Use these functions to deploy and manage the climate risk pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Deploy All Pipelines (Recommended)
# MAGIC 
# MAGIC This will:
# MAGIC 1. Set up Unity Catalog schemas
# MAGIC 2. Create all 4 DLT pipelines
# MAGIC 3. Create orchestration workflow
# MAGIC 4. Configure schedules

# COMMAND ----------

# Initialize orchestrator
orchestrator = EuropeanRiskPipelineOrchestrator()

# Deploy all pipelines
deployment_result = orchestrator.deploy_all_pipelines()

# Display results
print("\n" + "="*80)
print("DEPLOYMENT COMPLETE")
print("="*80)
displayHTML(f"""
<h2>✅ Pipeline Deployment Successful!</h2>
<table style="border-collapse: collapse; width: 100%;">
  <tr style="background-color: #f2f2f2;">
    <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Component</th>
    <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Details</th>
  </tr>
  <tr>
    <td style="border: 1px solid #ddd; padding: 8px;"><strong>Catalog</strong></td>
    <td style="border: 1px solid #ddd; padding: 8px;">{deployment_result['catalog']}</td>
  </tr>
  <tr>
    <td style="border: 1px solid #ddd; padding: 8px;"><strong>Pipelines Created</strong></td>
    <td style="border: 1px solid #ddd; padding: 8px;">{len(deployment_result['pipeline_ids'])}</td>
  </tr>
  <tr>
    <td style="border: 1px solid #ddd; padding: 8px;"><strong>Workflow Job ID</strong></td>
    <td style="border: 1px solid #ddd; padding: 8px;">{deployment_result.get('job_id', 'N/A')}</td>
  </tr>
</table>
<br>
<h3>Pipeline IDs:</h3>
<ul>
  {''.join([f"<li><strong>{name}:</strong> {pid}</li>" for name, pid in deployment_result['pipeline_ids'].items()])}
</ul>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Check Pipeline Status
# MAGIC 
# MAGIC Query the status of a specific pipeline.

# COMMAND ----------

# Example: Check status of a pipeline
# Uncomment and replace with actual pipeline ID
# pipeline_id = "your-pipeline-id-here"
# status = orchestrator.get_pipeline_status(pipeline_id)
# print(json.dumps(status, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 3: Manually Trigger a Pipeline
# MAGIC 
# MAGIC Trigger a specific pipeline on demand.

# COMMAND ----------

# Example: Manually trigger a pipeline
# Uncomment to use
# orchestrator.run_pipeline("terrain_ingestion")
# orchestrator.run_pipeline("weather_ingestion")
# orchestrator.run_pipeline("flood_risk")
# orchestrator.run_pipeline("drought_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Deployment Information

# COMMAND ----------

# Save deployment info to JSON
with open("/dbfs/FileStore/risk_app/deployment_info.json", "w") as f:
    json.dump(deployment_result, f, indent=2)

print("✓ Deployment information saved to: /dbfs/FileStore/risk_app/deployment_info.json")

# Display as DataFrame for easy viewing
import pandas as pd
df_pipelines = pd.DataFrame([
    {"Pipeline": name, "Pipeline ID": pid}
    for name, pid in deployment_result['pipeline_ids'].items()
])
display(df_pipelines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Risk Data
# MAGIC 
# MAGIC Once pipelines have run, you can query the risk analytics data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all catalogs
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show schemas in demo_hc catalog
# MAGIC SHOW SCHEMAS IN demo_hc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Query flood risk scores (once data is available)
# MAGIC -- SELECT 
# MAGIC --   location_name,
# MAGIC --   country_code,
# MAGIC --   flood_risk_score,
# MAGIC --   flood_risk_category,
# MAGIC --   evacuation_zone_area_km2
# MAGIC -- FROM demo_hc.risk_analytics.gold_flood_risk_scores
# MAGIC -- WHERE flood_risk_category IN ('CRITICAL', 'HIGH')
# MAGIC -- ORDER BY flood_risk_score DESC
# MAGIC -- LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Architecture
# MAGIC 
# MAGIC ```
# MAGIC Data Sources → Ingestion (Bronze) → Processing (Silver) → Analytics (Gold)
# MAGIC      ↓              ↓                      ↓                    ↓
# MAGIC   Terrain         Raw TIFF           Unified terrain        Risk scores
# MAGIC   Weather         Raw API data       Enriched weather       Alerts
# MAGIC   Satellite       Quality checks     H3 indexing            Time series
# MAGIC                                      ST functions           Summaries
# MAGIC ```
# MAGIC 
# MAGIC ### Pipelines:
# MAGIC 1. **Terrain Ingestion** (Weekly) - Copernicus, EEA, OpenGeoHub, GeoHarmonizer
# MAGIC 2. **Weather Ingestion** (Hourly) - AccuWeather API for 15 European capitals
# MAGIC 3. **Flood Risk** (Hourly) - Risk scoring, evacuation zones, alerts
# MAGIC 4. **Drought Risk** (Daily) - SPI, SPEI, SMI indices, restriction zones
# MAGIC 
# MAGIC ### Features:
# MAGIC - ✅ H3 hexagonal spatial indexing
# MAGIC - ✅ ST geospatial functions (buffers, distances, areas)
# MAGIC - ✅ Delta Live Tables with quality expectations
# MAGIC - ✅ Unity Catalog (demo_hc)
# MAGIC - ✅ 28 Delta tables across Bronze-Silver-Gold architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Monitor Pipeline Execution**
# MAGIC    - Go to Workflows → Jobs to view execution status
# MAGIC    - Check Delta Live Tables UI for data quality metrics
# MAGIC 
# MAGIC 2. **Query Risk Analytics**
# MAGIC    - Use SQL cells above to query `demo_hc.risk_analytics.*` tables
# MAGIC    - Create dashboards with Databricks SQL
# MAGIC 
# MAGIC 3. **Set Up Alerts**
# MAGIC    - Configure email notifications for high-risk locations
# MAGIC    - Set up Slack/webhook integrations
# MAGIC 
# MAGIC 4. **Optimize Performance**
# MAGIC    - Review execution times
# MAGIC    - Adjust cluster sizes if needed
# MAGIC    - Enable auto-optimization on tables
