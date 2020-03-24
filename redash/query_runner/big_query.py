import datetime
import logging
import sys
import time
from base64 import b64decode
import json

import httplib2
import requests

from redash import settings
from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

try:
    import apiclient.errors
    from apiclient.discovery import build
    from apiclient.errors import HttpError
    from oauth2client.service_account import ServiceAccountCredentials

    enabled = True
except ImportError:
    enabled = False

types_map = {
    'INTEGER': TYPE_INTEGER,
    'FLOAT': TYPE_FLOAT,
    'BOOLEAN': TYPE_BOOLEAN,
    'STRING': TYPE_STRING,
    'TIMESTAMP': TYPE_DATETIME,
}


def transform_cell(field_type, cell_value):
    if cell_value is None:
        return None
    if field_type == 'INTEGER':
        return int(cell_value)
    elif field_type == 'FLOAT':
        return float(cell_value)
    elif field_type == 'BOOLEAN':
        return cell_value.lower() == "true"
    elif field_type == 'TIMESTAMP':
        return datetime.datetime.fromtimestamp(float(cell_value))
    return cell_value


def transform_row(row, fields):
    row_data = {}

    for column_index, cell in enumerate(row["f"]):
        field = fields[column_index]
        if field.get('mode') == 'REPEATED':
            cell_value = [transform_cell(field['type'], item['v']) for item in cell['v']]
        else:
            cell_value = transform_cell(field['type'], cell['v'])

        row_data[field["name"]] = cell_value

    return row_data


def _load_key(filename):
    f = file(filename, "rb")
    try:
        return f.read()
    finally:
        f.close()


def _get_query_results(jobs, project_id, location, job_id, start_index):
    query_reply = jobs.getQueryResults(projectId=project_id,
                                       location=location,
                                       jobId=job_id,
                                       startIndex=start_index).execute()
    logging.debug('query_reply %s', query_reply)
    if not query_reply['jobComplete']:
        time.sleep(10)
        return _get_query_results(jobs, project_id, location, job_id, start_index)

    return query_reply


class BigQuery(BaseQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'projectId': {
                    'type': 'string',
                    'title': 'Project ID'
                },
                'additionalProjects': {
                    'type': 'string',
                    'title': 'Additional projects for schema read'
                },
                'jsonKeyFile': {
                    "type": "string",
                    'title': 'JSON Key File'
                },
                'totalMBytesProcessedLimit': {
                    "type": "number",
                    'title': 'Scanned Data Limit (MB)'
                },
                'userDefinedFunctionResourceUri': {
                    "type": "string",
                    'title': 'UDF Source URIs (i.e. gs://bucket/date_utils.js, gs://bucket/string_utils.js )'
                },
                'useStandardSql': {
                    "type": "boolean",
                    'title': "Use Standard SQL",
                    "default": True,
                },
                'location': {
                    "type": "string",
                    "title": "Processing Location",
                },
                'loadSchema': {
                    "type": "boolean",
                    "title": "Load Schema"
                },
                'previewMaxResults': {
                    "type": "number",
                    "title": "Preview Max Results",
                    "default": 100
                },
                "maxResults": {
                    "type": "number",
                    "title": "Max results retrieved",
                    "default": 10000
                },
                'maximumBillingTier': {
                    "type": "number",
                    "title": "Maximum Billing Tier"
                }
            },
            'required': ['jsonKeyFile', 'projectId'],
            "order": ['projectId', 'jsonKeyFile', 'loadSchema', 'useStandardSql', 'location', 'previewMaxResults', 'totalMBytesProcessedLimit', 'maximumBillingTier', 'userDefinedFunctionResourceUri'],
            'secret': ['jsonKeyFile']
        }

    def _get_bigquery_service(self):
        scope = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive"
        ]

        key = json_loads(b64decode(self.configuration['jsonKeyFile']))

        creds = ServiceAccountCredentials.from_json_keyfile_dict(key, scope)
        http = httplib2.Http(timeout=settings.BIGQUERY_HTTP_TIMEOUT)
        http = creds.authorize(http)

        return build("bigquery", "v2", http=http)

    def _get_project_id(self):
        return self.configuration["projectId"]

    def _get_location(self):
        return self.configuration.get("location")

    def _get_preview_max_results(self):
        return self.configuration.get("previewMaxResults", 100)

    def _get_max_results(self):
        return self.configuration.get("maxResults", 10000)

    def _get_additional_projects(self):
        if self.configuration.get("additionalProjects"):
            return map(str.strip, self.configuration["additionalProjects"].split(","))
        else:
            return []

    def _get_total_bytes_processed(self, jobs, query):
        job_data = {
            "query": query,
            "dryRun": True,
        }

        if self._get_location():
            job_data['location'] = self._get_location()

        if self.configuration.get('useStandardSql', False):
            job_data['useLegacySql'] = False

        response = jobs.query(projectId=self._get_project_id(), body=job_data).execute()
        return int(response["totalBytesProcessed"])

    def _get_job_data(self, query):
        job_data = {
            "configuration": {
                "query": {
                    "query": query,
                }
            }
        }

        if self._get_location():
            job_data['jobReference'] = {
                'location': self._get_location()
            }

        if self.configuration.get('useStandardSql', False):
            job_data['configuration']['query']['useLegacySql'] = False

        if self.configuration.get('userDefinedFunctionResourceUri'):
            resource_uris = self.configuration["userDefinedFunctionResourceUri"].split(',')
            job_data["configuration"]["query"]["userDefinedFunctionResources"] = map(
                lambda resource_uri: {"resourceUri": resource_uri}, resource_uris)

        if "maximumBillingTier" in self.configuration:
            job_data["configuration"]["query"]["maximumBillingTier"] = self.configuration["maximumBillingTier"]

        return job_data

    def _get_query_result(self, jobs, query):
        project_id = self._get_project_id()
        job_data = self._get_job_data(query)
        insert_response = jobs.insert(projectId=project_id, body=job_data).execute()
        current_row = 0
        query_reply = _get_query_results(jobs, project_id=project_id, location=self._get_location(),
                                         job_id=insert_response['jobReference']['jobId'], start_index=current_row)

        logger.debug("bigquery replied: %s", query_reply)

        rows = []

        max_returned_results = self._get_max_results()

        while ("rows" in query_reply) and \
                current_row < query_reply['totalRows'] and \
                len(rows) < max_returned_results:
            for row in query_reply["rows"]:
                rows.append(transform_row(row, query_reply["schema"]["fields"]))

            current_row += len(query_reply['rows'])

            query_result_request = {
                'projectId': project_id,
                'jobId': query_reply['jobReference']['jobId'],
                'startIndex': current_row
            }

            if self._get_location():
                query_result_request['location'] = self._get_location()

            query_reply = jobs.getQueryResults(**query_result_request).execute()

        columns = [{
            'name': f["name"],
            'friendly_name': f["name"],
            'type': "string" if f.get('mode') == "REPEATED"
            else types_map.get(f['type'], "string")
        } for f in query_reply["schema"]["fields"]]

        data = {
            "columns": columns,
            "rows": rows,
            'metadata': {'data_scanned': int(query_reply['totalBytesProcessed'])}
        }

        return data

    def _get_columns_schema(self, table_data, table_type, project_id, is_primary):
        columns = []
        for column in table_data.get('schema', {}).get('fields', []):
            columns.extend(self._get_columns_schema_column(column))

        if is_primary:
            table_name = table_data['id'].replace("%s:" % project_id, "")
        else:
            # Keep the project_id in, just replace the separator with the .
            table_name = table_data['id'].replace(":", ".")

        return {
            'name': table_name,
            'columns': columns,
            'type': table_type,
            # Only tables can be previewed on big query
            'preview': table_type == "TABLE"
        }

    def _get_columns_schema_column(self, column):
        columns = []
        if column['type'] == 'RECORD':
            for field in column['fields']:
                columns.append(u"{}.{}".format(column['name'], field['name']))
        else:
            columns.append(column['name'])

        return columns

    def _list_datasets(self):
        service = self._get_bigquery_service()
        primary_project_id = self._get_project_id()

        projects = set(self._get_additional_projects())
        # Always add the primary project to the the project list
        # (it's probably already there, but just in case it isn't)
        projects.add(primary_project_id)

        # Query for all datasets in a single round trip to minimize latency and cost
        query = "\nUNION ALL\n".join([
            "SELECT CATALOG_NAME, SCHEMA_NAME " \
            "FROM `%s`.INFORMATION_SCHEMA.SCHEMATA" % (project_id,)
            for project_id in projects
        ])

        # Submit the query to BigQuery.
        resp = service.jobs().query(projectId=primary_project_id, body={
            'useLegacySql':False,
            'query': query
        }).execute()

        # Format results as
        # [(project_id, dataset_id)]
        return map(
            lambda row: (row['f'][0]['v'], row['f'][1]['v']),
            resp.get("rows", [])
        )

    def _get_table_columns(self, dataset_ids):
        service = self._get_bigquery_service()
        primary_project_id = self._get_project_id()

        query_parts = []

        # Construct a long chain of SELECTs joined by UNION ALLs to retrieve all the tables and
        # structures in a single round trip, for minimum latency and cost
        for project_id, dataset_id in dataset_ids:
            query_parts.append(
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, STRING_AGG(COLUMN_NAME) "
                "FROM `%(project_id)s`.`%(dataset_id)s`.INFORMATION_SCHEMA.COLUMNS "
                "JOIN `%(project_id)s`.`%(dataset_id)s`.INFORMATION_SCHEMA.TABLES "
                "USING (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) "
                "GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE" % {
                    "project_id": project_id,
                    "dataset_id": dataset_id
                })

        query = "\nUNION ALL\n".join(query_parts)

        # Run the query on BigQuery.
        resp = service.jobs().query(projectId=primary_project_id, body={
            'useLegacySql':False,
            'query': query
        }).execute()

        # format the result rows as
        # [(project_id,dataset_id,table_name, table_type, table_columns)]
        return map(
            lambda row: (
                row['f'][0]['v'],
                row['f'][1]['v'],
                row['f'][2]['v'],
                row['f'][3]['v'],
                row['f'][4]['v'].split(",")
            ),
            resp.get("rows", [])
        )



    def get_schema(self, get_stats=False):
        if not self.configuration.get('loadSchema', False):
            return []

        primary_project_id = self._get_project_id()

        start = time.time()

        dataset_ids = self._list_datasets()

        table_columns = self._get_table_columns(dataset_ids)

        schema = []

        for project_id, dataset_id, table_name, table_type, table_columns in table_columns:
            if project_id == primary_project_id:
                full_table_name = dataset_id + "." + table_name
            else:
                full_table_name = project_id + "." + dataset_id + "." + table_name

            schema.append({
                'name': full_table_name,
                'columns': table_columns,
                # Big Query specific(for now)
                'type': table_type,
                # Views can't be previewed
                'preview': table_type != "VIEW"
            })

        logger.info("refresh took: %.2f", time.time() - start)

        return schema

    def run_query(self, query, user):
        logger.debug("BigQuery got query: %s", query)

        bigquery_service = self._get_bigquery_service()
        jobs = bigquery_service.jobs()

        try:
            if "totalMBytesProcessedLimit" in self.configuration:
                limitMB = self.configuration["totalMBytesProcessedLimit"]
                processedMB = self._get_total_bytes_processed(jobs, query) / 1000.0 / 1000.0
                if limitMB < processedMB:
                    return None, "Larger than %d MBytes will be processed (%f MBytes)" % (limitMB, processedMB)

            data = self._get_query_result(jobs, query)
            error = None

            json_data = json_dumps(data, ignore_nan=True)
        except apiclient.errors.HttpError as e:
            json_data = None
            if e.resp.status == 400:
                error = json_loads(e.content)['error']['message']
            else:
                error = e.content
        except KeyboardInterrupt:
            error = "Query cancelled by user."
            json_data = None

        return json_data, error

    @property
    def dry_run_support(self):
        return True

    def dry_run(self, query):
        bigquery_service = self._get_bigquery_service()
        project_id = self._get_project_id()

        args = {
            "query": query,
            "dryRun": True
        }

        if self.configuration.get('useStandardSql', False):
            args['useLegacySql'] = False

        try:
            return bigquery_service.jobs().query(projectId=project_id, body=args).execute()
        except HttpError as he:
            content = json.loads(he.content)
            return {
                "error": {
                    "code": content["error"]["code"],
                    "status": content["error"]["status"],
                    "message": content["error"]["message"]
                }
            }

    @property
    def supports_preview(self):
        return True

    def preview(self, full_table_name):
        bigquery_service = self._get_bigquery_service()
        # If the full_table_name has a project_id in it, extract it
        if full_table_name.count(".") == 2:
            project_id, _, full_table_name = full_table_name.partition(".")
        else:
            project_id = self._get_project_id()

        ds, _, table_name = full_table_name.partition(".")

        try:
            data = bigquery_service.tabledata().list(
                projectId=project_id,
                datasetId=ds,
                tableId=table_name,
                maxResults=self._get_preview_max_results()
            ).execute()
        except HttpError as he:
            content = json.loads(he.content)
            return {
                "error": {
                    "code": content["error"]["code"],
                    "status": content["error"]["status"],
                    "message": content["error"]["message"]
                }
            }

        columns = bigquery_service.tables().get(
            projectId=project_id,
            datasetId=ds,
            tableId=table_name
        ).execute()

        fields = columns["schema"]["fields"]

        def map_row(row):
            f = row["f"]

            row_data = {}
            for i, col in enumerate(f):
                row_data[fields[i]["name"]] = col["v"]

            return row_data

        def map_columns(column):
            return {
                "label": column["name"],
                "field": column["name"],
                "sort": "asc"
            }


        if "rows" not in data and data["totalRows"] == "0":
            data["rows"] = []

        return {
            "rows": list(map(map_row, data["rows"])),
            "columns": list(map(map_columns, fields))
        }

register(BigQuery)
