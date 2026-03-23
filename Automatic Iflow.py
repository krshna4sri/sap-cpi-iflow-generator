# PHASE 3: MULTI-TEMPLATE CPI IFLOW CLONER

import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import streamlit as st

APP_TITLE = "SAP CPI iFlow Generator - Multi Template"

# -------------------------------------------------------------------
# Template registry
# Update the file names below to match the ZIPs you keep locally,
# or upload one manually from the UI.
# -------------------------------------------------------------------
TEMPLATES: Dict[str, Dict[str, str]] = {
    "GET": {
        "default_zip": "Smartapp Get Journal Entry Basic Multi.zip",
        "display_name": "Smartapp Get Journal Entry Basic Multi",
        "artifact_id": "Smartapp_Get_Journal_Entry_Basic_Multi",
        "description": "Get Journal Entry",
        "sender_path": "/JournalEntry/Multi",
        "service_name": "API_JOURNALENTRYITEMBASIC_SRV",
        "odata_address": "https://my401471-api.s4hana.cloud.sap/sap/opu/odata/sap/API_JOURNALENTRYITEMBASIC_SRV",
        "entity_name": "A_JournalEntryItemBasic",
        "supports_safe_adapter_edits": "yes",
    },
    "CREATE": {
        "default_zip": "Smartapp Create Project Elements_Billingelement test.zip",
        "display_name": "Smartapp Create Project Elements_Billingelement test",
        "artifact_id": "Smartapp_Create_Project_Elements_Billingelement_test",
        "description": "Project Elements Create",
        "sender_path": "/ProjectElements/Create",
        "service_name": "",
        "odata_address": "",
        "entity_name": "",
        "supports_safe_adapter_edits": "no",
    },
    "UPDATE": {
        "default_zip": "Smartapp Update Project Elements_PCE.zip",
        "display_name": "Smartapp Update Project Elements_PCE",
        "artifact_id": "Smartapp_Update_Project_Elements_PCE",
        "description": "Smartapp Update Project Elements",
        "sender_path": "/ProjectElements/Update",
        "service_name": "",
        "odata_address": "",
        "entity_name": "",
        "supports_safe_adapter_edits": "no",
    },
    "DELETE": {
        "default_zip": "Smartapp Delete Purchase Order.zip",
        "display_name": "Smartapp Delete Purchase Order",
        "artifact_id": "Smartapp_to_SAP_PurchaseOrder_Delete",
        "description": "Delete Purchase Order",
        "sender_path": "/PurchaseOrder/Delete",
        "service_name": "",
        "odata_address": "",
        "entity_name": "",
        "supports_safe_adapter_edits": "no",
    },
}

OBJECT_PRESETS: Dict[str, Dict[str, Dict[str, str]]] = {
    "GET": {
        "Company Code": {
            "description": "GET Company Code data from SAP",
            "sender_path": "/companycode/get",
            "service_name": TEMPLATES["GET"]["service_name"],
            "odata_address": TEMPLATES["GET"]["odata_address"],
            "entity_name": "A_CompanyCode",
        },
        "Journal Entry": {
            "description": "GET Journal Entry data from SAP",
            "sender_path": "/journalentry/get",
            "service_name": TEMPLATES["GET"]["service_name"],
            "odata_address": TEMPLATES["GET"]["odata_address"],
            "entity_name": TEMPLATES["GET"]["entity_name"],
        },
    },
    "CREATE": {
        "Project Elements": {
            "description": "Create Project Elements",
            "sender_path": "/projectelements/create",
            "service_name": "",
            "odata_address": "",
            "entity_name": "",
        },
    },
    "UPDATE": {
        "Project Elements": {
            "description": "Update Project Elements",
            "sender_path": "/projectelements/update",
            "service_name": "",
            "odata_address": "",
            "entity_name": "",
        },
    },
    "DELETE": {
        "Purchase Order": {
            "description": "Delete Purchase Order",
            "sender_path": "/purchaseorder/delete",
            "service_name": "",
            "odata_address": "",
            "entity_name": "",
        },
    },
}

TEXT_FILE_SUFFIXES = {
    ".iflw", ".groovy", ".prop", ".propdef", ".mf", ".edmx",
    ".xsd", ".wsdl", ".mmap", ".project", ".xml"
}


@dataclass
class CloneRequest:
    operation: str
    object_name: str
    iflow_name: str
    artifact_id: str
    description: str
    sender_path: str
    entity_name: str
    service_name: str
    odata_address: str
    rename_only: bool = False


def safe_slug(text: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", text.strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "iflow"


class MultiTemplateIFlowGenerator:
    def __init__(self, template_zip_path: str, template_meta: Dict[str, str]):
        self.template_zip_path = template_zip_path
        self.template_meta = template_meta

    def extract(self, tmpdir: str) -> Path:
        root = Path(tmpdir) / "template"
        with zipfile.ZipFile(self.template_zip_path, "r") as zf:
            zf.extractall(root)
        return root

    def find_iflow(self, root: Path) -> Path:
        matches = list(root.rglob("*.iflw"))
        if not matches:
            raise FileNotFoundError("No .iflw file found in the template ZIP.")
        return matches[0]

    def rename_iflow_file(self, iflow_path: Path, new_name: str) -> Path:
        new_path = iflow_path.with_name(f"{safe_slug(new_name)}.iflw")
        if iflow_path != new_path:
            iflow_path.rename(new_path)
        return new_path

    def _read_text(self, path: Path) -> Optional[str]:
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return None

    def _write_text(self, path: Path, content: str) -> None:
        path.write_text(content, encoding="utf-8")

    def _replace_in_file(self, path: Path, replacements: List[Tuple[str, str]]) -> bool:
        content = self._read_text(path)
        if content is None:
            return False
        updated = content
        for old, new in replacements:
            if old:
                updated = updated.replace(old, new)
        if updated != content:
            self._write_text(path, updated)
            return True
        return False

    def _collect_replacements(self, request: CloneRequest) -> List[Tuple[str, str]]:
        replacements: List[Tuple[str, str]] = [
            (self.template_meta.get("display_name", ""), request.iflow_name),
            (self.template_meta.get("artifact_id", ""), request.artifact_id),
            (self.template_meta.get("description", ""), request.description),
        ]

        allow_adapter_edits = (
            self.template_meta.get("supports_safe_adapter_edits") == "yes"
            and request.operation == "GET"
            and not request.rename_only
        )
        if allow_adapter_edits:
            replacements.extend([
                (self.template_meta.get("sender_path", ""), request.sender_path),
                (self.template_meta.get("entity_name", ""), request.entity_name),
            ])
        return replacements

    def apply_updates(self, root: Path, request: CloneRequest) -> Dict[str, List[str]]:
        replacements = self._collect_replacements(request)
        touched_files: List[str] = []
        adapter_hits: List[str] = []

        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_FILE_SUFFIXES:
                continue
            changed = self._replace_in_file(path, replacements)
            if changed:
                rel = str(path.relative_to(root))
                touched_files.append(rel)
                text = self._read_text(path) or ""
                if request.sender_path and request.sender_path in text:
                    adapter_hits.append(rel)
                if request.entity_name and request.entity_name in text:
                    adapter_hits.append(rel)

        return {
            "touched_files": touched_files,
            "adapter_hits": sorted(set(adapter_hits)),
        }

    def package_zip(self, root: Path) -> bytes:
        out = Path(tempfile.gettempdir()) / f"iflow_{next(tempfile._get_candidate_names())}.zip"
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in root.rglob("*"):
                if path.is_file():
                    zf.write(path, path.relative_to(root).as_posix())
        return out.read_bytes()

    def generate(self, request: CloneRequest) -> Tuple[bytes, Dict[str, object]]:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self.extract(tmpdir)
            iflow = self.find_iflow(root)
            renamed_iflow = self.rename_iflow_file(iflow, request.iflow_name)
            results = self.apply_updates(root, request)
            zip_bytes = self.package_zip(root)
            allow_adapter_edits = (
                self.template_meta.get("supports_safe_adapter_edits") == "yes"
                and request.operation == "GET"
                and not request.rename_only
            )
            audit = {
                "iflow_file": str(renamed_iflow.relative_to(root)),
                "request": {
                    "operation": request.operation,
                    "object_name": request.object_name,
                    "iflow_name": request.iflow_name,
                    "artifact_id": request.artifact_id,
                    "description": request.description,
                    "sender_path": request.sender_path,
                    "entity_name": request.entity_name,
                    "rename_only": request.rename_only,
                },
                "notes": [
                    f"Operation {request.operation} uses its own base template.",
                    "CREATE/UPDATE/DELETE are clone-only by design in this version.",
                    "GET supports safe sender path + entity updates only within the existing template service context.",
                    "Service name, OData URL, mappings, WSDL/XSD, and deeper resources are preserved from the template.",
                    f"Safe adapter edits applied: {'yes' if allow_adapter_edits else 'no'}",
                ],
                **results,
            }
            return zip_bytes, audit


def get_default_template_path(operation: str) -> Optional[Path]:
    candidate = Path(TEMPLATES[operation]["default_zip"])
    if candidate.exists():
        return candidate
    return None


# -----------------------------
# Streamlit UI
# -----------------------------
def build_request() -> Tuple[CloneRequest, Optional[Path], Dict[str, str], Optional[bytes]]:
    operation = st.selectbox("Operation", ["GET", "CREATE", "UPDATE", "DELETE"])
    template_meta = TEMPLATES[operation]
    object_options = list(OBJECT_PRESETS[operation].keys())
    object_name = st.selectbox("Business object", object_options)
    preset = OBJECT_PRESETS[operation][object_name]

    default_template_path = get_default_template_path(operation)
    with st.sidebar:
        st.header("Template")
        st.caption(f"Selected operation: {operation}")
        uploaded = st.file_uploader(
            f"Upload {operation} template ZIP",
            type=["zip"],
            key=f"upload_{operation}",
        )
        if default_template_path:
            st.success(f"Default template found: {default_template_path.name}")
        else:
            st.info("No local default template found. Upload a ZIP here.")

    iflow_name = st.text_input("iFlow Name", f"{operation.title()} {object_name}")
    artifact_id_default = safe_slug(iflow_name)
    artifact_id = st.text_input("Artifact ID (must be unique in CPI)", artifact_id_default)
    description = st.text_area("Description", preset["description"])
    sender_path = st.text_input("HTTPS sender path", preset["sender_path"])

    rename_only_default = operation != "GET"
    rename_only = st.checkbox(
        "Rename only (recommended for CREATE/UPDATE/DELETE)",
        value=rename_only_default,
    )

    if operation == "GET":
        entity_name = st.text_input("Entity name", preset["entity_name"])
        st.text_input(
            "OData service name (preserved from template)",
            value=preset["service_name"],
            disabled=True,
        )
        st.text_input(
            "OData address / URL (preserved from template)",
            value=preset["odata_address"],
            disabled=True,
        )
    else:
        entity_name = ""
        st.text_input("Entity name", value="Not used for this operation in current version", disabled=True)
        st.text_input("Service / backend config", value="Preserved from template", disabled=True)

    request = CloneRequest(
        operation=operation,
        object_name=object_name,
        iflow_name=iflow_name,
        artifact_id=artifact_id,
        description=description,
        sender_path=sender_path,
        entity_name=entity_name,
        service_name=preset["service_name"],
        odata_address=preset["odata_address"],
        rename_only=rename_only,
    )

    uploaded_bytes = uploaded.getvalue() if uploaded is not None else None
    return request, default_template_path, template_meta, uploaded_bytes


def resolve_template(default_template_path: Optional[Path], uploaded_bytes: Optional[bytes], operation: str) -> Path:
    if uploaded_bytes is not None:
        temp_zip = Path(tempfile.gettempdir()) / f"user_template_{operation.lower()}.zip"
        temp_zip.write_bytes(uploaded_bytes)
        return temp_zip
    if default_template_path is not None and default_template_path.exists():
        return default_template_path
    raise FileNotFoundError(f"No {operation} template available. Upload the template ZIP.")


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Multi-template CPI iFlow generator for GET, CREATE, UPDATE, and DELETE. Use a unique Artifact ID for every upload.")

    st.markdown(
        """
        This version:
        - supports separate templates for GET, CREATE, UPDATE, and DELETE
        - clones each selected template into a new CPI-uploadable ZIP
        - safely updates only metadata by default
        - for GET only, can also safely update sender path and entity name inside the existing template service context
        - preserves mappings and deeper backend resources from the original template
        """
    )

    request, default_template_path, template_meta, uploaded_bytes = build_request()

    if request.operation in {"CREATE", "UPDATE", "DELETE"}:
        st.warning(
            f"{request.operation} currently works in clone-only mode. That is intentional because mappings and payload structures can vary unpredictably."
        )

    if st.button("Generate iFlow ZIP", type="primary"):
        try:
            template_path = resolve_template(default_template_path, uploaded_bytes, request.operation)
            generator = MultiTemplateIFlowGenerator(str(template_path), template_meta)
            zip_bytes, audit = generator.generate(request)
            st.success("Generated successfully.")
            st.download_button(
                "Download iFlow ZIP",
                data=zip_bytes,
                file_name=f"{safe_slug(request.iflow_name)}.zip",
                mime="application/zip",
            )
            with st.expander("Generation details"):
                st.json(audit)
        except Exception as exc:
            st.exception(exc)


if __name__ == "__main__":
    main()
