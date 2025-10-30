{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fb4b7588-9018-4c6c-9f8d-1aeda0548643",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Generated CSVs in: data/sample_output\n"
     ]
    }
   ],
   "source": [
    "# data/generate_data.py\n",
    "# Usage: python generate_data.py\n",
    "import csv\n",
    "import random\n",
    "from datetime import datetime, timedelta\n",
    "import os\n",
    "\n",
    "OUT_DIR = \"data/sample_output\"\n",
    "os.makedirs(OUT_DIR, exist_ok=True)\n",
    "\n",
    "NUM_PATIENTS = 1000\n",
    "NUM_RECORDS = 20000\n",
    "NUM_DEVICES = 10\n",
    "\n",
    "# generate patients.csv\n",
    "patients = []\n",
    "for pid in range(1, NUM_PATIENTS + 1):\n",
    "    patients.append({\n",
    "        \"patient_id\": pid,\n",
    "        \"name\": f\"Patient_{pid}\",\n",
    "        \"age\": random.randint(1, 90),\n",
    "        \"gender\": random.choice([\"M\", \"F\", \"Other\"]),\n",
    "        \"city\": random.choice([\"Nagpur\",\"Pune\",\"Mumbai\",\"Hyderabad\",\"Bangalore\"])\n",
    "    })\n",
    "\n",
    "with open(os.path.join(OUT_DIR, \"patients.csv\"), \"w\", newline=\"\") as f:\n",
    "    writer = csv.DictWriter(f, fieldnames=[\"patient_id\",\"name\",\"age\",\"gender\",\"city\"])\n",
    "    writer.writeheader()\n",
    "    writer.writerows(patients)\n",
    "\n",
    "# generate devices.csv\n",
    "devices = []\n",
    "for did in range(1, NUM_DEVICES + 1):\n",
    "    devices.append({\n",
    "        \"device_id\": f\"DEV_{did:02d}\",\n",
    "        \"device_type\": random.choice([\"TypeA\",\"TypeB\",\"TypeC\"]),\n",
    "        \"location\": random.choice([\"LabA\",\"LabB\",\"LabC\"]),\n",
    "        \"installed_on\": (datetime.now() - timedelta(days=random.randint(30,365))).strftime(\"%Y-%m-%d\")\n",
    "    })\n",
    "\n",
    "with open(os.path.join(OUT_DIR, \"devices.csv\"), \"w\", newline=\"\") as f:\n",
    "    writer = csv.DictWriter(f, fieldnames=[\"device_id\",\"device_type\",\"location\",\"installed_on\"])\n",
    "    writer.writeheader()\n",
    "    writer.writerows(devices)\n",
    "\n",
    "# generate test_logs.csv\n",
    "start_ts = datetime.now() - timedelta(days=90)\n",
    "test_status_choices = [\"OK\", \"ERROR\", \"RETRY\"]\n",
    "with open(os.path.join(OUT_DIR, \"test_logs.csv\"), \"w\", newline=\"\") as f:\n",
    "    fieldnames = [\"test_id\", \"patient_id\", \"device_id\", \"test_timestamp\", \"test_duration_seconds\", \"test_value\", \"status\", \"error_code\"]\n",
    "    writer = csv.DictWriter(f, fieldnames=fieldnames)\n",
    "    writer.writeheader()\n",
    "    for i in range(1, NUM_RECORDS + 1):\n",
    "        ts = start_ts + timedelta(seconds=random.randint(0, 90*24*3600))\n",
    "        status = random.choices(test_status_choices, weights=[85,10,5])[0]\n",
    "        error_code = \"\"\n",
    "        if status != \"OK\":\n",
    "            error_code = random.choice([\"E101\",\"E202\",\"E303\",\"E404\"])\n",
    "        writer.writerow({\n",
    "            \"test_id\": f\"TST_{i:06d}\",\n",
    "            \"patient_id\": random.randint(1, NUM_PATIENTS),\n",
    "            \"device_id\": random.choice([d[\"device_id\"] for d in devices]),\n",
    "            \"test_timestamp\": ts.strftime(\"%Y-%m-%d %H:%M:%S\"),\n",
    "            \"test_duration_seconds\": round(random.uniform(10.0, 600.0), 2),\n",
    "            \"test_value\": round(random.uniform(0.1, 500.0), 2),\n",
    "            \"status\": status,\n",
    "            \"error_code\": error_code\n",
    "        })\n",
    "\n",
    "print(\"Generated CSVs in:\", OUT_DIR)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9c98af35-c4ad-4223-9789-b223db2e7711",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "generate_data",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
