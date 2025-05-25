import shutil
from pathlib import Path
from prefect import flow
from src.utils.base_ingestion import BaseIngestionWorkflow

class ImportWebHoldWorkflow(BaseIngestionWorkflow):
    def __init__(self):
        super().__init__(
            name="Import Web Hold",
            target_table="borarch.holdweb",
            field_mappings={
                "date": "date",
                "fund_name": "fund_name",
                "name": "name",
                "sector": "sector",
                "currency": "currency",
                "units": "units",
                "cost": "cost",
                "mv": "mv"
            },
            procedure_name="bormeta.usp_holdweb_process",
            truncate_before_load=True
        )

@flow
def import_web_hold_flow(
    source_file: str,
    db_host: str,
    db_port: str,  # Accept as string for env var compatibility
    db_user: str,
    db_password: str,
    db_name: str,
    delimiter: str = ',',
    quote_char: str = '"',
    line_terminator: str = '\n',
    skip_lines: int = 1,
    truncate_before_load: bool = False,  # <-- Add this line, default matches YAML
) -> bool:
    wf = ImportWebHoldWorkflow()
    return wf.execute(
        file_path=source_file,
        db_host=db_host,
        db_port=db_port,  # Will be cast to int in workflow
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        delimiter=delimiter,
        quote_char=quote_char,
        line_terminator=line_terminator,
        skip_lines=skip_lines,
        truncate_before_load=truncate_before_load,  # <-- Pass it through
    ) 