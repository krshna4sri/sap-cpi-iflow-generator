"""
SAP CPI iFlow Intelligence Generator — v3.2 (Manifest Fix)
════════════════════════════════════════════════════════════
v3.2 fixes:
  FIX — MANIFEST.MF now matches exact SAP CPI format:
        SAP-BundleType: IntegrationFlow  (not IFlow)
        Full Import-Package OSGi block   (required by CPI parser)
        SAP-RuntimeProfile, SAP-NodeType (required fields)
        Strategy: for cloned ZIPs, copy original MANIFEST and only
                  replace name/ID fields. For skeletons, use hardcoded
                  real manifest template.
"""

import io
import json
import os
import re
import base64
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import streamlit as st

# ─── Paths ────────────────────────────────────────────────────────────────────
TEMPLATE_DIR = Path("template_library")
INDEX_FILE   = Path("trained_index/iflow_index.json")
OUTPUT_DIR   = Path("output_zips")
PACKAGE_DIR  = Path("package_library")

for d in [TEMPLATE_DIR, INDEX_FILE.parent, OUTPUT_DIR, PACKAGE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")

TEXT_SUFFIXES = {
    ".iflw", ".groovy", ".prop", ".propdef", ".mf",
    ".edmx", ".xsd", ".wsdl", ".mmap", ".project", ".xml"
}

# ═══════════════════════════════════════════════════════════════════════════════
# CORRECT MANIFEST.MF
# Verified against real SAP CPI package exports.
# Critical fields CPI parser requires:
#   • SAP-BundleType: IntegrationFlow  (NOT "IFlow")
#   • Import-Package: full OSGi package list (CPI won't load without this)
#   • SAP-RuntimeProfile: iflmap
#   • SAP-NodeType: IFLMAP
#   • \r\n line endings throughout (CRLF — Java OSGi parser requirement)
#   • Continuation lines start with exactly one space
#   • File must end with \r\n\r\n (blank line)
# ═══════════════════════════════════════════════════════════════════════════════

# The Import-Package and Import-Service blocks are identical across all iFlows
# in real CPI exports — they are OSGi runtime requirements, not iFlow-specific.
_IMPORT_PACKAGE = (
    "Import-Package: com.sap.esb.application.services.cxf.interceptor,com.sap\r\n"
    " .esb.security,com.sap.it.op.agent.api,com.sap.it.op.agent.collector.cam\r\n"
    " el,com.sap.it.op.agent.collector.cxf,com.sap.it.op.agent.mpl,javax.jms,\r\n"
    " javax.jws,javax.wsdl,javax.xml.bind.annotation,javax.xml.namespace,java\r\n"
    " x.xml.ws,org.apache.camel;version=\"2.8\",org.apache.camel.builder;versio\r\n"
    " n=\"2.8\",org.apache.camel.builder.xml;version=\"2.8\",org.apache.camel.com\r\n"
    " ponent.cxf,org.apache.camel.model;version=\"2.8\",org.apache.camel.proces\r\n"
    " sor;version=\"2.8\",org.apache.camel.processor.aggregate;version=\"2.8\",or\r\n"
    " g.apache.camel.spring.spi;version=\"2.8\",org.apache.commons.logging,org.\r\n"
    " apache.cxf.binding,org.apache.cxf.binding.soap,org.apache.cxf.binding.s\r\n"
    " oap.spring,org.apache.cxf.bus,org.apache.cxf.bus.resource,org.apache.cx\r\n"
    " f.bus.spring,org.apache.cxf.buslifecycle,org.apache.cxf.catalog,org.apa\r\n"
    " che.cxf.configuration.jsse;version=\"2.5\",org.apache.cxf.configuration.s\r\n"
    " pring,org.apache.cxf.endpoint,org.apache.cxf.headers,org.apache.cxf.int\r\n"
    " erceptor,org.apache.cxf.management.counters;version=\"2.5\",org.apache.cx\r\n"
    " f.message,org.apache.cxf.phase,org.apache.cxf.resource,org.apache.cxf.s\r\n"
    " ervice.factory,org.apache.cxf.service.model,org.apache.cxf.transport,or\r\n"
    " g.apache.cxf.transport.common.gzip,org.apache.cxf.transport.http,org.ap\r\n"
    " ache.cxf.transport.http.policy,org.apache.cxf.workqueue,org.apache.cxf.\r\n"
    " ws.rm.persistence,org.apache.cxf.wsdl11,org.osgi.framework;version=\"1.6\r\n"
    " .0\",org.slf4j;version=\"1.6\",org.springframework.beans.factory.config;ve\r\n"
    " rsion=\"3.0\",com.sap.esb.camel.security.cms,org.apache.camel.spi,com.sap\r\n"
    " .esb.webservice.audit.log,com.sap.esb.camel.endpoint.configurator.api,c\r\n"
    " om.sap.esb.camel.jdbc.idempotency.reorg,javax.sql,org.apache.camel.proc\r\n"
    " essor.idempotent.jdbc,org.osgi.service.blueprint;version=\"[1.0.0,2.0.0)\r\n"
    " \"\r\n"
)

_IMPORT_SERVICE = (
    "Import-Service: com.sap.esb.webservice.audit.log.AuditLogger,com.sap.esb\r\n"
    " .security.KeyManagerFactory;multiple:=false,com.sap.esb.security.TrustM\r\n"
    " anagerFactory;multiple:=false,javax.sql.DataSource;multiple:=false;filt\r\n"
    " er=\"(dataSourceName=default)\",org.apache.cxf.ws.rm.persistence.RMStore;\r\n"
    " multiple:=false,com.sap.esb.camel.security.cms.SignatureSplitter;multip\r\n"
    " le:=false\r\n"
)


def make_manifest(artifact_id: str, iflow_name: str) -> bytes:
    """
    Build a MANIFEST.MF that SAP CPI accepts and auto-populates the upload dialog.

    CPI auto-fill behaviour (verified from real exports):
      ID field   <- Bundle-SymbolicName  (underscore_safe, no spaces)
      Name field <- Bundle-Name          (human readable, spaces OK)

    Result: user uploads ZIP, both Name and ID pre-populate automatically.
    User just clicks Add — no manual typing needed.
    """
    # ID: underscores (Bundle-SymbolicName)
    # Name: spaces   (Bundle-Name / Origin-Bundle-Name)
    display_name = artifact_id.replace("_", " ")

    content = (
        f"Manifest-Version: 1.0\r\n"
        f"Bundle-SymbolicName: {artifact_id}\r\n"
        f"Bundle-ManifestVersion: 2\r\n"
        f"Origin-Bundle-SymbolicName: {artifact_id}\r\n"
        f"SAP-ArtifactTrait: \r\n"
        + _IMPORT_PACKAGE
        + f"Origin-Bundle-Name: {display_name}\r\n"
        f"SAP-RuntimeProfile: iflmap\r\n"
        f"Bundle-Name: {display_name}\r\n"
        f"Bundle-Version: 1.0.0\r\n"
        f"SAP-NodeType: IFLMAP\r\n"
        f"SAP-BundleType: IntegrationFlow\r\n"
        + _IMPORT_SERVICE
        + f"Origin-Bundle-Version: 1.0.0\r\n"
        f"\r\n"
    )
    return content.encode("utf-8")


def patch_manifest(original_mf_bytes: bytes, artifact_id: str, iflow_name: str) -> bytes:
    """
    For cloned ZIPs: copy original MANIFEST.MF and only replace
    the name/ID fields. This preserves all OSGi packages from the real iFlow.
    """
    try:
        mf = original_mf_bytes.decode("utf-8", errors="replace")
        # Replace Bundle-SymbolicName
        mf = re.sub(r'(Bundle-SymbolicName:\s*)([^\r\n]+)',
                    f'Bundle-SymbolicName: {artifact_id}', mf)
        # Replace Origin-Bundle-SymbolicName
        mf = re.sub(r'(Origin-Bundle-SymbolicName:\s*)([^\r\n]+)',
                    f'Origin-Bundle-SymbolicName: {artifact_id}', mf)
        # Replace Bundle-Name (single line, not continuation)
        mf = re.sub(r'(Bundle-Name:\s*)([^\r\n]+)',
                    f'Bundle-Name: {iflow_name}', mf)
        # Replace Origin-Bundle-Name
        mf = re.sub(r'(Origin-Bundle-Name:\s*)([^\r\n]+)',
                    f'Origin-Bundle-Name: {iflow_name}', mf)
        # Reset version to 1.0.0
        mf = re.sub(r'(Bundle-Version:\s*)([^\r\n]+)',
                    'Bundle-Version: 1.0.0', mf)
        mf = re.sub(r'(Origin-Bundle-Version:\s*)([^\r\n]+)',
                    'Origin-Bundle-Version: 1.0.0', mf)
        return mf.encode("utf-8")
    except Exception:
        # If patching fails, generate a fresh correct manifest
        return make_manifest(artifact_id, iflow_name)


def make_project(artifact_id: str) -> bytes:
    return f'''<?xml version="1.0" encoding="UTF-8"?><projectDescription>
   <n>{artifact_id}</n>
   <comment/>
   <projects/>
   <buildSpec>
      <buildCommand>
         <n>org.eclipse.jdt.core.javabuilder</n>
         <arguments/>
      </buildCommand>
   </buildSpec>
   <natures>
      <nature>org.eclipse.jdt.core.javanature</nature>
      <nature>com.sap.ide.ifl.project.support.project.nature</nature>
      <nature>com.sap.ide.ifl.bsn</nature>
   </natures>
</projectDescription>'''.encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN SKELETON iFlow XMLs (fallback only — real templates preferred)
# ═══════════════════════════════════════════════════════════════════════════════

def _skeleton(method: str, odata_op: str) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<bpmn2:definitions xmlns:bpmn2="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:ifl="http:///com.sap.ifl.model/Ifl.xsd"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  id="Definitions_1">
  <bpmn2:collaboration id="Collaboration_1" name="Default Collaboration">
    <bpmn2:participant id="Sender"  name="Sender_HTTPS"  processRef="SenderProcess"/>
    <bpmn2:participant id="Recv"    name="Receiver_OData" processRef="ReceiverProcess"/>
    <bpmn2:participant id="IP"      name="%%IFLOW_NAME%%"  processRef="IntegrationProcess"/>
    <bpmn2:messageFlow id="MFin"  sourceRef="Sender" targetRef="StartEvent_1"/>
    <bpmn2:messageFlow id="MFout" sourceRef="EndEvent_1" targetRef="Recv"/>
  </bpmn2:collaboration>
  <bpmn2:process id="SenderProcess"   name="Sender_HTTPS"  isExecutable="false"/>
  <bpmn2:process id="ReceiverProcess" name="Receiver_OData" isExecutable="false"/>
  <bpmn2:process id="IntegrationProcess" name="%%IFLOW_NAME%%" isExecutable="true">
    <bpmn2:extensionElements>
      <ifl:property><key>ComponentType</key><value>IFlow</value></ifl:property>
    </bpmn2:extensionElements>
    <bpmn2:startEvent id="StartEvent_1" name="Start">
      <bpmn2:extensionElements>
        <ifl:property><key>ComponentType</key><value>StartEvent</value></ifl:property>
        <ifl:property><key>address</key><value>%%SENDER_PATH%%</value></ifl:property>
        <ifl:property><key>httpMethod</key><value>{method}</value></ifl:property>
        <ifl:property><key>enableBasicAuthentication</key><value>true</value></ifl:property>
      </bpmn2:extensionElements>
      <bpmn2:outgoing>Seq1</bpmn2:outgoing>
    </bpmn2:startEvent>
    <bpmn2:serviceTask id="ContentModifier_1" name="Set_Headers">
      <bpmn2:extensionElements>
        <ifl:property><key>ComponentType</key><value>ContentModifier</value></ifl:property>
        <ifl:property><key>messageHeaderTable</key><value>Accept:application/json</value></ifl:property>
      </bpmn2:extensionElements>
      <bpmn2:incoming>Seq1</bpmn2:incoming>
      <bpmn2:outgoing>Seq2</bpmn2:outgoing>
    </bpmn2:serviceTask>
    <bpmn2:scriptTask id="GroovyScript_1" name="Transform" scriptFormat="groovy">
      <bpmn2:extensionElements>
        <ifl:property><key>ComponentType</key><value>ScriptTask</value></ifl:property>
        <ifl:property><key>scriptFile</key><value>script/%%IFLOW_NAME%%_transform.groovy</value></ifl:property>
      </bpmn2:extensionElements>
      <bpmn2:incoming>Seq2</bpmn2:incoming>
      <bpmn2:outgoing>Seq3</bpmn2:outgoing>
    </bpmn2:scriptTask>
    <bpmn2:serviceTask id="ODataReceiver_1" name="{odata_op}_%%ENTITY_NAME%%">
      <bpmn2:extensionElements>
        <ifl:property><key>ComponentType</key><value>OData</value></ifl:property>
        <ifl:property><key>address</key><value>%%ODATA_ADDRESS%%</value></ifl:property>
        <ifl:property><key>entitySetName</key><value>%%ENTITY_NAME%%</value></ifl:property>
        <ifl:property><key>operation</key><value>{odata_op}</value></ifl:property>
        <ifl:property><key>authenticationMethod</key><value>BasicAuthentication</value></ifl:property>
        <ifl:property><key>credentialName</key><value>S4HANA_CRED</value></ifl:property>
      </bpmn2:extensionElements>
      <bpmn2:incoming>Seq3</bpmn2:incoming>
      <bpmn2:outgoing>Seq4</bpmn2:outgoing>
    </bpmn2:serviceTask>
    <bpmn2:endEvent id="EndEvent_1" name="End">
      <bpmn2:extensionElements>
        <ifl:property><key>ComponentType</key><value>EndEvent</value></ifl:property>
      </bpmn2:extensionElements>
      <bpmn2:incoming>Seq4</bpmn2:incoming>
    </bpmn2:endEvent>
    <bpmn2:sequenceFlow id="Seq1" sourceRef="StartEvent_1"     targetRef="ContentModifier_1"/>
    <bpmn2:sequenceFlow id="Seq2" sourceRef="ContentModifier_1" targetRef="GroovyScript_1"/>
    <bpmn2:sequenceFlow id="Seq3" sourceRef="GroovyScript_1"   targetRef="ODataReceiver_1"/>
    <bpmn2:sequenceFlow id="Seq4" sourceRef="ODataReceiver_1"  targetRef="EndEvent_1"/>
  </bpmn2:process>
</bpmn2:definitions>'''

SKELETONS = {
    "GET":    _skeleton("GET",    "GET"),
    "CREATE": _skeleton("POST",   "CREATE"),
    "UPDATE": _skeleton("PUT",    "UPDATE"),
    "DELETE": _skeleton("DELETE", "DELETE"),
}

# ═══════════════════════════════════════════════════════════════════════════════
# GROOVY PATTERN LIBRARY
# ═══════════════════════════════════════════════════════════════════════════════

GROOVY_PATTERNS = {
"GET": '''\
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

// GET Response Handler — Entity: {ENTITY} | Path: {PATH}
def Message processData(Message message) {
    try {
        def body = message.getBody(String.class)
        def log  = messageLogFactory.getMessageLog(message)
        if (log) log.addAttachmentAsString("GET_Response", body, "application/json")

        def json    = new JsonSlurper().parseText(body)
        def records = json?.d?.results ?: (json?.value ?: [])
        def output  = records.collect { r -> [
            // TODO: map {ENTITY} fields here
            id          : r?.ObjectID    ?: r?.ID          ?: "",
            description : r?.Description ?: r?.Name        ?: "",
            status      : r?.Status      ?: "UNKNOWN",
            createdAt   : r?.CreatedAt   ?: "",
        ]}
        message.setBody(JsonOutput.toJson([results: output, count: output.size()]))
        message.setHeader("Content-Type",   "application/json")
        message.setHeader("X-Record-Count", output.size().toString())
    } catch (Exception e) {
        message.setBody(JsonOutput.toJson([error: "GET failed", detail: e.getMessage()]))
        throw new Exception("GET handler error: " + e.getMessage(), e)
    }
    return message
}''',

"CREATE": '''\
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.text.SimpleDateFormat

// CREATE Payload Builder — Entity: {ENTITY} | Path: {PATH}
def Message processData(Message message) {
    try {
        def body  = message.getBody(String.class)
        def log   = messageLogFactory.getMessageLog(message)
        if (log) log.addAttachmentAsString("CREATE_Input", body, "application/json")
        def input = new JsonSlurper().parseText(body)
        if (!input) throw new Exception("Empty input payload")
        def now = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())
        def payload = [
            // TODO: map {ENTITY} fields here
            ExternalID  : input?.id          ?: "",
            Description : input?.description ?: "",
            Status      : input?.status      ?: "ACTIVE",
            CreatedBy   : "CPI_INTEGRATION",
            CreatedAt   : now,
        ]
        if (!payload.ExternalID) throw new Exception("Required field 'id' is missing")
        message.setBody(JsonOutput.toJson(payload))
        message.setHeader("Content-Type", "application/json")
        message.setProperty("createdKey", payload.ExternalID)
    } catch (Exception e) {
        message.setBody(JsonOutput.toJson([error: "CREATE failed", detail: e.getMessage()]))
        throw new Exception("CREATE handler error: " + e.getMessage(), e)
    }
    return message
}''',

"UPDATE": '''\
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.text.SimpleDateFormat

// UPDATE Payload Builder — Entity: {ENTITY} | Path: {PATH}
def Message processData(Message message) {
    try {
        def body    = message.getBody(String.class)
        def headers = message.getHeaders()
        def input   = new JsonSlurper().parseText(body)
        if (!input) throw new Exception("Empty input payload")
        def key = input?.id ?: input?.ObjectID ?: headers?.get("entityKey") ?: ""
        if (!key) throw new Exception("Entity key not found in UPDATE request")
        def now = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())
        def payload = [:]
        if (input?.description != null) payload.Description = input.description
        if (input?.status      != null) payload.Status      = input.status
        payload.LastModifiedAt = now
        payload.LastModifiedBy = "CPI_INTEGRATION"
        message.setProperty("entityKey", key)
        message.setBody(JsonOutput.toJson(payload))
        message.setHeader("Content-Type",  "application/json")
        message.setHeader("X-HTTP-Method", "PATCH")
    } catch (Exception e) {
        message.setBody(JsonOutput.toJson([error: "UPDATE failed", detail: e.getMessage()]))
        throw new Exception("UPDATE handler error: " + e.getMessage(), e)
    }
    return message
}''',

"DELETE": '''\
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

// DELETE Key Extractor — Entity: {ENTITY} | Path: {PATH}
def Message processData(Message message) {
    try {
        def body    = message.getBody(String.class)
        def headers = message.getHeaders()
        def key     = ""
        if (body?.trim()?.startsWith("{")) {
            def input = new JsonSlurper().parseText(body)
            key = input?.id ?: input?.ObjectID ?: input?.key ?: ""
        }
        if (!key) key = headers?.get("X-Entity-Key") ?: ""
        if (!key) throw new Exception("Entity key not found")
        def log = messageLogFactory.getMessageLog(message)
        if (log) log.addAttachmentAsString("DELETE_Key", "Deleting {ENTITY}: " + key, "text/plain")
        message.setProperty("entityKey",  key)
        message.setProperty("entityName", "{ENTITY}")
        message.setHeader("X-Entity-Key",  key)
        message.setHeader("X-HTTP-Method", "DELETE")
        message.setBody("")
    } catch (Exception e) {
        message.setBody(JsonOutput.toJson([error: "DELETE failed", detail: e.getMessage()]))
        throw new Exception("DELETE handler error: " + e.getMessage(), e)
    }
    return message
}''',

"PASSTHROUGH": '''\
import com.sap.gateway.ip.core.customdev.util.Message

// Passthrough with Audit Logging
def Message processData(Message message) {
    try {
        def body = message.getBody(String.class)
        def log  = messageLogFactory.getMessageLog(message)
        if (log) {
            log.addAttachmentAsString("Payload", body, "text/plain")
            log.setStringProperty("Payload-Size", body?.length()?.toString() ?: "0")
        }
        message.setHeader("X-Source",    "SAP-CPI-PASSTHROUGH")
        message.setHeader("X-Processed", new Date().toString())
    } catch (Exception e) {
        throw new Exception("Passthrough error: " + e.getMessage(), e)
    }
    return message
}''',
}


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY
# ═══════════════════════════════════════════════════════════════════════════════

def safe_slug(text: str) -> str:
    v = re.sub(r"[^A-Za-z0-9_]+", "_", str(text).strip())
    v = re.sub(r"_+", "_", v).strip("_")
    # Artifact ID cannot start with a digit
    if v and v[0].isdigit():
        v = "iFlow_" + v
    return v or "iFlow"


def detect_operation(xml: str, filename: str = "") -> str:
    x = xml.upper()
    # Look for explicit httpMethod first
    m = re.search(r'<KEY>HTTPMETHOD</KEY>\s*<VALUE>([^<]+)</VALUE>', x)
    if not m:
        m = re.search(r'HTTPMETHOD</IFL:KEY>\s*<IFL:VALUE>([^<]+)</IFL:VALUE>', x)
    if m:
        method = m.group(1).strip()
        if method == "POST":   return "CREATE"
        if method in ("PUT","PATCH"): return "UPDATE"
        if method == "DELETE": return "DELETE"
        if method == "GET":    return "GET"
    # OData operation property
    if "<VALUE>CREATE</VALUE>" in x or "<IFL:VALUE>CREATE</IFL:VALUE>" in x: return "CREATE"
    if "<VALUE>UPDATE</VALUE>" in x or "<IFL:VALUE>UPDATE</IFL:VALUE>" in x: return "UPDATE"
    if "<VALUE>DELETE</VALUE>" in x or "<IFL:VALUE>DELETE</IFL:VALUE>" in x: return "DELETE"
    # Filename hints (only if XML gave nothing)
    fn = filename.upper()
    if any(w in fn for w in ["_CREATE_","CREATE_","_CREATE","_POST_","POST_"]):  return "CREATE"
    if any(w in fn for w in ["_UPDATE_","UPDATE_","_PUT_","_PATCH_"]):           return "UPDATE"
    if any(w in fn for w in ["_DELETE_","DELETE_","_REMOVE_"]):                  return "DELETE"
    return "GET"


def extract_xml_props(xml: str) -> Dict:
    p = {
        "sender_path":"", "entity_name":"", "odata_address":"",
        "sender_adapter":"HTTPS", "receiver_adapter":"OData",
        "credential_name":"", "iflow_display_name":"",
    }
    # Address — collect all, classify
    for m in re.finditer(
            r'<(?:ifl:)?key>address</(?:ifl:)?key>\s*<(?:ifl:)?value>([^<]+)</(?:ifl:)?value>',
            xml, re.IGNORECASE):
        v = m.group(1).strip()
        if v.startswith("http"): p["odata_address"] = v
        elif v.startswith("/"):  p["sender_path"]   = v

    m = re.search(
        r'<(?:ifl:)?key>entitySetName</(?:ifl:)?key>\s*<(?:ifl:)?value>([^<]+)</(?:ifl:)?value>',
        xml, re.IGNORECASE)
    if m: p["entity_name"] = m.group(1).strip()

    m = re.search(
        r'<(?:ifl:)?key>credentialName</(?:ifl:)?key>\s*<(?:ifl:)?value>([^<]+)</(?:ifl:)?value>',
        xml, re.IGNORECASE)
    if m: p["credential_name"] = m.group(1).strip()

    m = re.search(r'<bpmn2:process[^>]+name="([^"]+)"[^>]+isExecutable="true"', xml)
    if m: p["iflow_display_name"] = m.group(1).strip()

    x = xml.upper()
    if   "SFTP"         in x: p["sender_adapter"] = "SFTP"
    elif "PROCESSDIRECT" in x: p["sender_adapter"] = "ProcessDirect"
    elif "IDOC"         in x: p["sender_adapter"] = "IDoc"
    elif "AS2"          in x: p["sender_adapter"] = "AS2"
    if   "SOAP" in x: p["receiver_adapter"] = "SOAP"
    elif "RFC"  in x: p["receiver_adapter"] = "RFC"
    return p


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 2 — PACKAGE-LEVEL ZIP UNWRAPPER
# Your uploaded ZIPs are SAP CPI package exports, NOT single iFlow exports.
# Structure: outer.zip → {hash}_content (each is an inner iFlow ZIP)
#            + resources.cnt (base64 JSON mapping hash → name/type)
# We unwrap each inner ZIP and treat it as a separate template.
# ═══════════════════════════════════════════════════════════════════════════════

def is_cpi_package_export(zip_bytes: bytes) -> bool:
    """Detect if this is a CPI package-level export (has resources.cnt)."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            return any(n.endswith("resources.cnt") for n in zf.namelist())
    except Exception:
        return False


def unwrap_package_export(zip_bytes: bytes, package_name: str) -> List[Dict]:
    """
    Unwrap a CPI package export into a list of individual iFlow records.
    Returns list of {name, id, bytes} dicts.
    """
    records = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as outer:
            names = outer.namelist()
            # Find resources.cnt (may be at root or inside a subfolder)
            cnt_name = next((n for n in names if n.endswith("resources.cnt")), None)
            if not cnt_name:
                return records

            cnt_raw  = outer.read(cnt_name)
            try:
                resources = json.loads(base64.b64decode(cnt_raw).decode("utf-8"))
                res_list  = resources.get("resources", [])
            except Exception:
                return records

            # Build hash→name map, filter to IFlow type only
            hash_map = {}
            for r in res_list:
                if r.get("resourceType") == "IFlow":
                    hash_map[r["id"]] = {
                        "name":      r.get("name", r["id"]).replace(".zip",""),
                        "unique_id": r.get("uniqueId", safe_slug(r.get("name","iflow"))),
                    }

            # Read each _content file that maps to an IFlow
            prefix = str(Path(cnt_name).parent)
            prefix = "" if prefix == "." else prefix + "/"

            for hash_id, meta in hash_map.items():
                content_name = f"{prefix}{hash_id}_content"
                if content_name not in names:
                    # Try without prefix
                    content_name = f"{hash_id}_content"
                if content_name not in names:
                    continue
                try:
                    content_bytes = outer.read(content_name)
                    if content_bytes[:2] == b"PK":  # it's a ZIP
                        records.append({
                            "name":  meta["name"],
                            "id":    safe_slug(meta["unique_id"]),
                            "bytes": content_bytes,
                        })
                except Exception:
                    pass
    except Exception:
        pass
    return records


def parse_iflow_zip(zip_bytes: bytes, name: str) -> Dict:
    """Parse a single direct iFlow ZIP (has .iflw inside)."""
    rec = {
        "filename": name, "id": safe_slug(Path(name).stem),
        "name": Path(name).stem, "operation": "GET",
        "xml": "", "groovy_scripts": [], "has_mapping": False,
        "iflow_files": [], "groovy_files": [], "file_size": len(zip_bytes),
        "props": {},
    }
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            all_names = zf.namelist()
            rec["has_mapping"] = any(n.endswith(".mmap") for n in all_names)
            rec["iflow_files"] = [n for n in all_names if n.endswith(".iflw")]
            rec["groovy_files"]= [n for n in all_names if n.endswith(".groovy")]
            for n in all_names:
                if n.endswith(".iflw"):
                    xml = zf.read(n).decode("utf-8", errors="replace")
                    rec["xml"]       = xml
                    rec["props"]     = extract_xml_props(xml)
                    rec["operation"] = detect_operation(xml, name)
                    break
            for n in all_names:
                if n.endswith(".groovy"):
                    try:
                        code = zf.read(n).decode("utf-8", errors="replace")
                        rec["groovy_scripts"].append({"file": n, "code": code})
                    except Exception:
                        pass
    except Exception as e:
        rec["parse_error"] = str(e)
    return rec


def process_uploaded_file(zip_bytes: bytes, filename: str) -> List[Dict]:
    """
    Handle both formats:
    - CPI package export (resources.cnt) → unwrap and parse each inner iFlow
    - Single iFlow ZIP (.iflw inside)    → parse directly
    """
    if is_cpi_package_export(zip_bytes):
        inner_flows = unwrap_package_export(zip_bytes, Path(filename).stem)
        results = []
        for flow in inner_flows:
            rec = parse_iflow_zip(flow["bytes"], flow["name"] + ".zip")
            rec["id"]   = flow["id"]
            rec["name"] = flow["name"]
            rec["raw_zip_bytes"] = flow["bytes"]  # store for cloning
            results.append(rec)
        return results
    else:
        rec = parse_iflow_zip(zip_bytes, filename)
        rec["raw_zip_bytes"] = zip_bytes
        return [rec]


# ═══════════════════════════════════════════════════════════════════════════════
# INDEX
# ═══════════════════════════════════════════════════════════════════════════════

def load_index() -> List[Dict]:
    if INDEX_FILE.exists():
        try:
            return json.loads(INDEX_FILE.read_text())
        except Exception:
            pass
    return []


def save_index(records: List[Dict]) -> None:
    slim = []
    for r in records:
        s = {k: v for k, v in r.items() if k not in ("xml", "raw_zip_bytes")}
        s["xml_preview"] = r.get("xml","")[:600]
        slim.append(s)
    INDEX_FILE.write_text(json.dumps(slim, indent=2))


def find_best_match(index: List[Dict], operation: str,
                    entity_hint: str = "",
                    path_hint: str = "",
                    sender_adapter: str = "HTTPS") -> Optional[Dict]:
    """
    Score each template and return the best match.
    Scoring:
      +10  operation matches (GET/CREATE/UPDATE/DELETE)
      +8   sender adapter matches (HTTPS vs ProcessDirect etc)
      +5   entity name contains hint
      +3   sender path contains hint
      +2   iFlow name contains operation keyword
      -5   penalty if operation does not match (avoids returning wrong type)

    Only returns a match if score > 0 (operation must match).
    """
    op = operation.upper()
    best = (0, None)
    for rec in index:
        score = 0
        rec_op = rec.get("operation","").upper()

        # Operation match is mandatory — must score > 0
        if rec_op == op:
            score += 10
        else:
            continue  # skip templates with wrong operation entirely

        # Adapter type match
        rec_adapter = rec.get("props",{}).get("sender_adapter","HTTPS")
        if sender_adapter and rec_adapter.upper() == sender_adapter.upper():
            score += 8

        # Entity name similarity
        rec_entity = rec.get("props",{}).get("entity_name","").lower()
        if entity_hint and entity_hint.lower() in rec_entity:
            score += 5
        elif entity_hint and rec_entity and rec_entity in entity_hint.lower():
            score += 2  # partial reverse match

        # Sender path similarity
        rec_path = rec.get("props",{}).get("sender_path","").lower()
        if path_hint and path_hint.lower() in rec_path:
            score += 3
        elif path_hint and rec_path and rec_path in path_hint.lower():
            score += 1

        if score > best[0]:
            best = (score, rec)
    return best[1]


def load_template_zip(template_id: str) -> Optional[bytes]:
    p = TEMPLATE_DIR / f"{template_id}.zip"
    return p.read_bytes() if p.exists() else None


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 1 (continued) — CORRECT ZIP BUILDER
# Uses make_manifest() and make_project() with correct CPI-compatible content
# ═══════════════════════════════════════════════════════════════════════════════

def build_zip_from_skeleton(iflow_name: str, artifact_id: str,
                             iflow_xml: str, groovy_code: str = "") -> bytes:
    """Build a fresh CPI-importable ZIP from a skeleton XML."""
    slug = safe_slug(iflow_name)
    out  = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("META-INF/MANIFEST.MF",
                    make_manifest(artifact_id, iflow_name))
        zf.writestr(".project",
                    make_project(artifact_id))
        zf.writestr(
            f"src/main/resources/scenarioflows/integrationflow/{slug}.iflw",
            iflow_xml.encode("utf-8"))
        if groovy_code:
            zf.writestr(
                f"src/main/resources/script/{slug}_transform.groovy",
                groovy_code.encode("utf-8"))
    return out.getvalue()


def clone_and_patch_zip(original_zip: bytes,
                         subs: List[Tuple[str, str]],
                         new_name: str,
                         artifact_id: str,
                         groovy_code: str = "") -> bytes:
    """
    MINIMAL CLONE — proven to work with SAP CPI import.

    Strategy (verified against real CPI package exports):
    - Copy every file byte-for-byte from the original ZIP
    - MANIFEST.MF: string-replace only name/ID/version values — preserve
      field order, line endings, Import-Package block, SAP-ArtifactTrait exactly
    - .project: replace only the <n> tag content
    - .iflw: rename file + apply text substitutions for name/ID
    - Groovy: inject new script if provided, otherwise keep original

    DO NOT rebuild MANIFEST from scratch — field order and exact byte format
    matters to the CPI OSGi parser.
    """
    out         = io.BytesIO()
    groovy_done = False
    slug        = safe_slug(artifact_id)  # use artifact_id for filename slug

    # Extract original symbolic name from manifest for substitution
    orig_symbolic = ""
    orig_display  = ""
    try:
        with zipfile.ZipFile(io.BytesIO(original_zip)) as _z:
            if "META-INF/MANIFEST.MF" in _z.namelist():
                _mf = _z.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
                _m  = re.search(r"Bundle-SymbolicName:\s*([^\r\n]+)", _mf)
                if _m: orig_symbolic = _m.group(1).strip()
                _m  = re.search(r"Bundle-Name:\s*([^\r\n]+)", _mf)
                if _m: orig_display  = _m.group(1).strip()
    except Exception:
        pass

    display_name = artifact_id.replace("_", " ")  # "Get_PurchaseOrder_iFlow" → "Get PurchaseOrder iFlow"

    def _patch_manifest(mf_bytes: bytes) -> bytes:
        mf = mf_bytes.decode("utf-8", "replace")
        # Replace by exact string match — preserves all surrounding bytes
        if orig_symbolic:
            mf = mf.replace(f"Bundle-SymbolicName: {orig_symbolic}",
                            f"Bundle-SymbolicName: {artifact_id}")
            mf = mf.replace(f"Origin-Bundle-SymbolicName: {orig_symbolic}",
                            f"Origin-Bundle-SymbolicName: {artifact_id}")
        if orig_display:
            mf = mf.replace(f"Bundle-Name: {orig_display}",
                            f"Bundle-Name: {display_name}")
            mf = mf.replace(f"Origin-Bundle-Name: {orig_display}",
                            f"Origin-Bundle-Name: {display_name}")
        # Reset version numbers
        mf = re.sub(r"(Bundle-Version: )[^\r\n]+",         f"\\g<1>1.0.0", mf)
        mf = re.sub(r"(Origin-Bundle-Version: )[^\r\n]+",  f"\\g<1>1.0.0", mf)
        return mf.encode("utf-8")

    def _patch_project(proj_bytes: bytes) -> bytes:
        proj = proj_bytes.decode("utf-8", "replace")
        proj = re.sub(r"<n>[^<]+</n>", f"<n>{artifact_id}</n>", proj)
        return proj.encode("utf-8")

    with zipfile.ZipFile(io.BytesIO(original_zip), "r") as zin:
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data  = zin.read(item.filename)
                fname = item.filename
                sfx   = Path(fname).suffix.lower()

                # MANIFEST — minimal string patch preserving exact format
                if fname == "META-INF/MANIFEST.MF":
                    zout.writestr(fname, _patch_manifest(data))
                    continue

                # .project — replace only <n> tag
                if fname == ".project":
                    zout.writestr(fname, _patch_project(data))
                    continue

                # Rename .iflw file
                if sfx == ".iflw":
                    parent = str(Path(fname).parent)
                    fname  = (f"{parent}/{slug}.iflw"
                               if parent != "." else f"{slug}.iflw")

                # Text substitutions in .iflw and text files
                if sfx in TEXT_SUFFIXES:
                    try:
                        txt = data.decode("utf-8")
                        # Apply caller-provided substitutions
                        for old, new in subs:
                            if old and old.strip() and old != new:
                                txt = txt.replace(old, new)
                        # Also substitute original name if detected
                        if orig_symbolic and orig_symbolic != artifact_id:
                            txt = txt.replace(orig_symbolic, artifact_id)
                        if orig_display and orig_display != display_name:
                            txt = txt.replace(orig_display, display_name)
                        data = txt.encode("utf-8")
                    except Exception:
                        pass

                # Groovy — replace first existing script with new code
                if sfx == ".groovy" and groovy_code and not groovy_done:
                    parent2 = str(Path(fname).parent)
                    fname   = (f"{parent2}/{slug}_transform.groovy"
                                if parent2 != "." else f"{slug}_transform.groovy")
                    data    = groovy_code.encode("utf-8")
                    groovy_done = True

                zout.writestr(fname, data)

            # Add Groovy if original had none
            if groovy_code and not groovy_done:
                zout.writestr(
                    f"src/main/resources/script/{slug}_transform.groovy",
                    groovy_code.encode("utf-8"))

    return out.getvalue()


def get_groovy(op: str, entity: str = "",
               path: str = "", extra: str = "") -> str:
    pat = GROOVY_PATTERNS.get(op.upper(), GROOVY_PATTERNS["PASSTHROUGH"])
    pat = pat.replace("{ENTITY}", entity or "Entity")
    pat = pat.replace("{PATH}",   path   or "/api/v1/resource")
    if extra and extra.strip():
        lines = "\n".join(f"//   {l}" for l in extra.strip().splitlines())
        pat  += f"\n\n// ── Additional requirements ──────────────────────\n{lines}\n"
    return pat


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 3 — CORRECTED INTENT PARSER
# "Create a GET iFlow" → GET  (verb "create" is about creating the artifact,
#                               not the OData operation)
# "Build a POST iFlow" → CREATE
# "Generate DELETE iFlow" → DELETE
# Rule: explicit HTTP method words (GET/POST/PUT/DELETE) in the text
#       take priority over action verbs (create/update/delete).
# ═══════════════════════════════════════════════════════════════════════════════

def parse_intent(text: str) -> Dict:
    t = text.lower()
    cfg = {
        "operation": "GET", "iflow_name": "",
        "sender_path": "", "entity_name": "", "odata_address": "",
        "sender_adapter": "HTTPS", "receiver_adapter": "OData",
        "groovy_needed": False, "groovy_req": "", "mapping": "1:1 root",
    }

    # ── Step 1: explicit HTTP method takes top priority ────────────────────
    op_from_method = None
    if re.search(r'\bget\s+i?flow\b|\bhttp\s+get\b|\bget\s+method\b|\bget\b.*iflow|\biflow\b.*\bget\b', t):
        op_from_method = "GET"
    if re.search(r'\bpost\s+i?flow\b|\bhttp\s+post\b|\bpost\s+method\b', t):
        op_from_method = "CREATE"
    if re.search(r'\bput\s+i?flow\b|\bpatch\s+i?flow\b|\bhttp\s+put\b|\bhttp\s+patch\b', t):
        op_from_method = "UPDATE"
    if re.search(r'\bdelete\s+i?flow\b|\bhttp\s+delete\b|\bdelete\s+method\b', t):
        op_from_method = "DELETE"

    # ── Step 2: "a GET iFlow", "GET-based", "using GET" patterns ──────────
    m = re.search(r'\b(get|post|put|patch|delete)\b[\s\-]+(i?flow|iflow|based|method|request)', t)
    if m:
        word_map = {"get":"GET","post":"CREATE","put":"UPDATE","patch":"UPDATE","delete":"DELETE"}
        op_from_method = word_map.get(m.group(1), op_from_method)

    # ── Step 3: action verb detection ONLY if no explicit HTTP method ──────
    if op_from_method:
        cfg["operation"] = op_from_method
    else:
        # Only use verb detection when no HTTP method clue found
        if any(w in t for w in ["create record","post record","insert record",
                                  "create entity","create new","send create",
                                  "create purchase","create sales","create supplier"]):
            cfg["operation"] = "CREATE"
        elif any(w in t for w in ["update record","put record","patch record",
                                    "modify record","update entity","update purchase",
                                    "update sales"]):
            cfg["operation"] = "UPDATE"
        elif any(w in t for w in ["delete record","remove record","delete entity",
                                    "delete purchase","delete sales"]):
            cfg["operation"] = "DELETE"
        else:
            cfg["operation"] = "GET"

    # ── Groovy ────────────────────────────────────────────────────────────
    if any(w in t for w in ["groovy","script","transform","convert","filter","map field"]):
        cfg["groovy_needed"] = True

    # ── Entity ────────────────────────────────────────────────────────────
    m = re.search(r'\bA_[A-Za-z]+\b', text)
    if m: cfg["entity_name"] = m.group(0)

    entity_map = {
        "purchase order": "A_PurchaseOrder",   "purchaseorder": "A_PurchaseOrder",
        "sales order":    "A_SalesOrder",       "salesorder":    "A_SalesOrder",
        "journal entry":  "A_JournalEntryItemBasic",
        "company code":   "A_CompanyCode",
        "business partner": "A_BusinessPartner",
        "material":       "A_Product",
        "project":        "A_WorkPackage",
        "supplier invoice":"A_SupplierInvoice",
        "work order":     "A_MaintenanceOrder",
    }
    for phrase, entity in entity_map.items():
        if phrase in t and not cfg["entity_name"]:
            cfg["entity_name"] = entity
            break

    # ── Sender path ───────────────────────────────────────────────────────
    m = re.search(r'(/[A-Za-z][A-Za-z0-9/_-]+)', text)
    if m: cfg["sender_path"] = m.group(1)
    elif cfg["entity_name"]:
        short = cfg["entity_name"].replace("A_","").replace("ItemBasic","")
        cfg["sender_path"] = f"/{short}/{cfg['operation'].title()}"

    # ── OData URL ─────────────────────────────────────────────────────────
    m = re.search(r'https?://\S+', text)
    if m: cfg["odata_address"] = m.group(0)

    # ── Adapter hints ─────────────────────────────────────────────────────
    if "sftp"          in t: cfg["sender_adapter"] = "SFTP"
    elif "processdirect" in t: cfg["sender_adapter"] = "ProcessDirect"
    if "soap"          in t: cfg["receiver_adapter"] = "SOAP"
    elif re.search(r'\bhttp\b', t) and "https" not in t:
        cfg["receiver_adapter"] = "HTTP"

    # ── iFlow name ────────────────────────────────────────────────────────
    m = re.search(r'"([^"]{3,60})"', text) or re.search(r"'([^']{3,60})'", text)
    if m:
        cfg["iflow_name"] = safe_slug(m.group(1))
    elif cfg["entity_name"]:
        short = cfg["entity_name"].replace("A_","")
        cfg["iflow_name"] = f"{cfg['operation'].title()}_{short}_iFlow"
    else:
        cfg["iflow_name"] = f"{cfg['operation'].title()}_iFlow"

    return cfg


# ═══════════════════════════════════════════════════════════════════════════════
# OLLAMA
# ═══════════════════════════════════════════════════════════════════════════════

def ollama_ok() -> bool:
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def ollama_stream(prompt: str) -> str:
    full = ""
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": True},
            stream=True, timeout=120,
        )
        for line in resp.iter_lines():
            if not line: continue
            try:
                d = json.loads(line)
                full += d.get("response","")
                if d.get("done"): break
            except Exception:
                continue
    except Exception as e:
        full = f"[Ollama error: {e}]"
    return full


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 4 — GENERATE iFlow — NEVER raises, always returns a ZIP
# ═══════════════════════════════════════════════════════════════════════════════

def generate_iflow(cfg: Dict, index: List[Dict]) -> Tuple[bytes, str, str]:
    op          = cfg["operation"]
    # artifact_id = underscore slug  (CPI ID field — no spaces allowed)
    # iflow_name  = same slug        (used internally; make_manifest converts to display name)
    raw_name    = cfg["iflow_name"] or f"{op}_iFlow"
    artifact_id = safe_slug(raw_name)   # e.g. Get_PurchaseOrder_iFlow
    iflow_name  = artifact_id           # keep consistent; make_manifest adds spaces for display
    sender_path = cfg["sender_path"] or f"/{safe_slug(raw_name.lower())}"
    entity_name = cfg["entity_name"] or "A_Entity"
    odata_addr  = cfg.get("odata_address","")

    matched    = find_best_match(index, op, entity_name, sender_path, cfg.get("sender_adapter","HTTPS"))
    tmpl_name  = matched["name"] if matched else f"Built-in {op} skeleton"
    tmpl_src   = "uploaded template" if matched else "built-in skeleton"

    # Groovy from pattern library
    groovy_code = ""
    if cfg.get("groovy_needed"):
        groovy_code = get_groovy(op, entity_name, sender_path,
                                  cfg.get("groovy_req",""))

    # Build ZIP
    if matched:
        original = load_template_zip(matched["id"])
        if original is None:
            # Try raw bytes stored in session (uploaded but not yet saved)
            original = None

        if original is not None:
            p = matched.get("props",{})
            old_name  = matched.get("props",{}).get("iflow_display_name","") or matched.get("name","")
            old_id    = matched.get("id","")
            subs = [
                (old_name,                     iflow_name),
                (old_id,                       artifact_id),
                (p.get("sender_path",""),      sender_path),
                (p.get("entity_name",""),      entity_name),
                (p.get("odata_address",""),    odata_addr),
                ("%%IFLOW_NAME%%",             iflow_name),
                ("%%SENDER_PATH%%",            sender_path),
                ("%%ENTITY_NAME%%",            entity_name),
                ("%%ODATA_ADDRESS%%",          odata_addr),
            ]
            zip_bytes = clone_and_patch_zip(original, subs, iflow_name,
                                             artifact_id, groovy_code)
        else:
            # Template ID exists in index but ZIP not on disk — use skeleton
            tmpl_src  = "built-in skeleton (template ZIP not found on disk)"
            xml = SKELETONS.get(op, SKELETONS["GET"])
            for ph, val in [("%%IFLOW_NAME%%",iflow_name),("%%SENDER_PATH%%",sender_path),
                            ("%%ENTITY_NAME%%",entity_name),("%%ODATA_ADDRESS%%",odata_addr)]:
                xml = xml.replace(ph, val)
            zip_bytes = build_zip_from_skeleton(iflow_name, artifact_id, xml, groovy_code)
    else:
        # No matching template — use skeleton (never raise error)
        xml = SKELETONS.get(op, SKELETONS["GET"])
        for ph, val in [("%%IFLOW_NAME%%",iflow_name),("%%SENDER_PATH%%",sender_path),
                        ("%%ENTITY_NAME%%",entity_name),("%%ODATA_ADDRESS%%",odata_addr)]:
            xml = xml.replace(ph, val)
        zip_bytes = build_zip_from_skeleton(iflow_name, artifact_id, xml, groovy_code)

    summary = f"""✅ **iFlow generated successfully**

| Field | Value |
|---|---|
| **iFlow Name** | `{iflow_name}` |
| **Artifact ID** | `{artifact_id}` |
| **Operation** | `{op}` |
| **Sender Path** | `{sender_path}` |
| **Entity** | `{entity_name}` |
| **OData URL** | `{odata_addr or "(not set)"}` |
| **Template Used** | {tmpl_name} |
| **Source** | {tmpl_src} |
| **Groovy** | {"✓ Included (" + op + " pattern)" if groovy_code else "Not included"} |
| **ZIP Size** | {round(len(zip_bytes)/1024,1)} KB |"""

    return zip_bytes, groovy_code, summary



# ═══════════════════════════════════════════════════════════════════════════════
# SMARTAPP ONE PACKAGE SUPPORT
# Generates a modified package export by recursively replacing hostname and
# credential values inside the trained/original package, including nested ZIPs.
# ═══════════════════════════════════════════════════════════════════════════════

def _replace_bytes_recursive(content: bytes, replacements: List[Tuple[str, str]]) -> Tuple[bytes, int]:
    modified_count = 0

    # Recurse into nested ZIPs
    if content[:2] == b"PK":
        try:
            src = io.BytesIO(content)
            out = io.BytesIO()
            with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    data = zin.read(item.filename)
                    new_data, nested_changes = _replace_bytes_recursive(data, replacements)
                    modified_count += nested_changes
                    zout.writestr(item, new_data)
            rebuilt = out.getvalue()
            if rebuilt != content:
                modified_count += 1
            return rebuilt, modified_count
        except Exception:
            pass

    new_content = content
    changed = False
    for old, new in replacements:
        if old and new and old != new:
            newer = new_content.replace(old.encode("utf-8"), new.encode("utf-8"))
            if newer != new_content:
                changed = True
                new_content = newer
    if changed:
        modified_count += 1
    return new_content, modified_count


def apply_replacements_to_package(zip_bytes: bytes, replacements: List[Tuple[str, str]]) -> Tuple[bytes, int]:
    return _replace_bytes_recursive(zip_bytes, replacements)


def smartapp_prompt_requested(prompt: str) -> bool:
    p = prompt.lower()
    return ("smartapp one" in p or "smartappone" in p) and ("package" in p or "generate" in p)


def parse_smartapp_replacements(prompt: str) -> Dict[str, str]:
    defaults = {
        "old_host": "my401471.s4hana.cloud.sap:443",
        "new_host": "my401478.s4hana.cloud.sap:443",
        "old_cred": "SAPCloud",
        "new_cred": "S4HANACred",
    }

    patterns = [
        (r'old\s*hostname\s*[:=]\s*([^,;\n]+)', 'old_host'),
        (r'new\s*hostname\s*[:=]\s*([^,;\n]+)', 'new_host'),
        (r'old\s*credential\s*[:=]\s*([^,;\n]+)', 'old_cred'),
        (r'new\s*credential\s*[:=]\s*([^,;\n]+)', 'new_cred'),
    ]
    for pat, key in patterns:
        m = re.search(pat, prompt, re.IGNORECASE)
        if m:
            defaults[key] = m.group(1).strip().strip("\"'")

    host_arrow = re.search(r'hostname\s*[:=]?\s*([^\s,;]+)\s*(?:->|to)\s*([^\s,;]+)', prompt, re.IGNORECASE)
    if host_arrow:
        defaults['old_host'], defaults['new_host'] = host_arrow.group(1), host_arrow.group(2)

    cred_arrow = re.search(r'credential(?:s)?\s*[:=]?\s*([^\s,;]+)\s*(?:->|to)\s*([^\s,;]+)', prompt, re.IGNORECASE)
    if cred_arrow:
        defaults['old_cred'], defaults['new_cred'] = cred_arrow.group(1), cred_arrow.group(2)

    return defaults


def _zip_looks_like_smartapp_package(zip_path: Path) -> bool:
    """Heuristic: package/file name or unpacked CPI resources indicate Smartapp ONE."""
    try:
        lname = zip_path.name.lower()
        if "smartapp" in lname and "one" in lname:
            return True

        data = zip_path.read_bytes()
        if is_cpi_package_export(data):
            for flow in unwrap_package_export(data, zip_path.stem):
                flow_name = str(flow.get("name", "")).lower()
                if "smartapp" in flow_name and "one" in flow_name:
                    return True
        else:
            rec = parse_iflow_zip(data, zip_path.name)
            search_blob = " ".join([
                rec.get("name", ""),
                rec.get("filename", ""),
                rec.get("props", {}).get("iflow_display_name", ""),
                rec.get("xml", "")[:3000],
            ]).lower()
            if "smartapp" in search_blob and "one" in search_blob:
                return True
    except Exception:
        pass
    return False


def locate_smartapp_package() -> Tuple[Optional[Path], str]:
    candidates: List[Tuple[Path, str]] = []

    # 1) Explicit uploaded working files / common locations
    for fp in [
        Path('Smartapp ONE.zip'),
        Path('/mnt/data/Smartapp ONE.zip'),
        Path.cwd() / 'Smartapp ONE.zip',
    ]:
        if fp.exists():
            candidates.append((fp, 'uploaded source file'))

    # 2) Any ZIP saved in the package library, even if the filename does not exactly match
    for fp in sorted(PACKAGE_DIR.glob('*.zip')):
        if _zip_looks_like_smartapp_package(fp):
            candidates.append((fp, 'package library'))

    # 3) Search template ZIPs created during training as a last resort
    for fp in sorted(TEMPLATE_DIR.glob('*.zip')):
        try:
            meta = fp.with_suffix('.meta.json')
            blob = fp.stem.lower()
            if meta.exists():
                blob += ' ' + meta.read_text(errors='ignore').lower()
            if 'smartapp' in blob and 'one' in blob:
                candidates.append((fp, 'trained template library'))
                continue
            if _zip_looks_like_smartapp_package(fp):
                candidates.append((fp, 'trained template library'))
        except Exception:
            pass

    # 4) Search the trained index metadata
    try:
        for rec in load_index():
            blob = json.dumps(rec, ensure_ascii=False).lower()
            if 'smartapp' in blob and 'one' in blob:
                tid = rec.get('id')
                if tid:
                    fp = TEMPLATE_DIR / f'{tid}.zip'
                    if fp.exists():
                        candidates.append((fp, 'trained index'))
    except Exception:
        pass

    seen = set()
    deduped: List[Tuple[Path, str]] = []
    for fp, source in candidates:
        key = str(fp.resolve()) if fp.exists() else str(fp)
        if key not in seen:
            seen.add(key)
            deduped.append((fp, source))

    if deduped:
        return deduped[0]
    return None, ''


def generate_smartapp_package(prompt: str, replacements_override: Optional[Dict[str, str]] = None) -> Tuple[bytes, str]:
    replacements_cfg = parse_smartapp_replacements(prompt)
    if replacements_override:
        for key, value in replacements_override.items():
            if key in replacements_cfg and value is not None and str(value).strip():
                replacements_cfg[key] = str(value).strip()
    package_path, source = locate_smartapp_package()
    if package_path is None:
        raise FileNotFoundError(
            'Smartapp ONE package not found. Upload or train the package first. '
            'The app searched the uploaded source file, package library, trained template library, and trained index.'
        )

    original = package_path.read_bytes()
    replacements = [
        (replacements_cfg['old_host'], replacements_cfg['new_host']),
        (replacements_cfg['old_cred'], replacements_cfg['new_cred']),
    ]
    new_zip, changed_files = apply_replacements_to_package(original, replacements)

    summary = f"""✅ **Smartapp ONE package generated successfully**

| Field | Value |
|---|---|
| **Source Package** | `{package_path.name}` |
| **Source Location** | {source} |
| **Old Hostname** | `{replacements_cfg['old_host']}` |
| **New Hostname** | `{replacements_cfg['new_host']}` |
| **Old Credential** | `{replacements_cfg['old_cred']}` |
| **New Credential** | `{replacements_cfg['new_cred']}` |
| **Changed Entries** | {changed_files} |
| **ZIP Size** | {round(len(new_zip)/1024, 1)} KB |
"""
    return new_zip, summary

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG & CSS
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="SAP CPI iFlow Generator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.stApp { background:#0d1117; color:#e6edf3; font-family:'Segoe UI',sans-serif; }
section[data-testid="stSidebar"] { background:#161b22 !important; border-right:1px solid #30363d; }
.msg-user { display:flex; justify-content:flex-end; margin:12px 0; }
.msg-user .bubble { background:#1f6feb; color:#fff; border-radius:18px 18px 4px 18px;
    padding:12px 18px; max-width:75%; font-size:14px; line-height:1.5; }
.msg-bot { display:flex; justify-content:flex-start; margin:12px 0; gap:10px; }
.msg-bot .avatar { width:32px; height:32px; background:linear-gradient(135deg,#238636,#1f6feb);
    border-radius:50%; display:flex; align-items:center; justify-content:center;
    font-size:16px; flex-shrink:0; }
.msg-bot .bubble { background:#161b22; border:1px solid #30363d;
    border-radius:4px 18px 18px 18px; padding:14px 18px; max-width:85%;
    font-size:14px; line-height:1.6; }
.chip { display:inline-block; padding:3px 10px; border-radius:12px;
    font-size:11px; font-weight:700; margin:2px; }
.chip-get    { background:#1a3b1a; color:#3fb950; border:1px solid #238636; }
.chip-create { background:#1f3a5f; color:#58a6ff; border:1px solid #388bfd; }
.chip-update { background:#3b2a1a; color:#d29922; border:1px solid #9e6a03; }
.chip-delete { background:#3b1a1a; color:#f85149; border:1px solid #da3633; }
.stButton>button { background:#21262d; color:#e6edf3; border:1px solid #30363d;
    border-radius:8px; font-weight:500; transition:all 0.2s; }
.stButton>button:hover { background:#30363d; border-color:#58a6ff; }
.stTextInput>div>div>input,
.stTextArea>div>div>textarea {
    background:#21262d !important; border:1px solid #30363d !important;
    color:#e6edf3 !important; border-radius:8px !important; }
div[data-testid="stExpander"] { background:#161b22; border:1px solid #30363d; border-radius:8px; }
div[data-testid="metric-container"] { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px; }
div[data-testid="metric-container"] label { color:#8b949e !important; }
div[data-testid="stMetricValue"] { color:#58a6ff !important; }
.sbox { border-radius:8px; padding:12px 16px; margin:6px 0; font-size:13px; }
.sbox-ok   { background:#1a3b1a; border:1px solid #238636; color:#3fb950; }
.sbox-warn { background:#3b2a1a; border:1px solid #9e6a03; color:#d29922; }
.sbox-info { background:#1f3a5f; border:1px solid #388bfd; color:#58a6ff; }
#MainMenu { visibility:hidden; } footer { visibility:hidden; } header { visibility:hidden; }
</style>
""", unsafe_allow_html=True)

# ─── Session state ─────────────────────────────────────────────────────────────
for k, v in [("messages",[]),("last_zip",None),("last_fname",None),
              ("pending_cfg",None),
              ("smartapp_old_host","my401471.s4hana.cloud.sap:443"),
              ("smartapp_new_host",""),
              ("smartapp_old_cred","SAPCloud"),
              ("smartapp_new_cred","")]:
    if k not in st.session_state:
        st.session_state[k] = v

# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚡ CPI iFlow Generator")
    st.markdown("---")
    mode = st.radio("Mode", [
        "💬 Chat & Generate",
        "📁 Upload iFlows",
        "🧠 Train Index",
    ], label_visibility="collapsed")
    st.markdown("---")

    index   = load_index()
    n_zips  = len(list(TEMPLATE_DIR.glob("*.zip")))
    op_cnts = defaultdict(int)
    for r in index: op_cnts[r.get("operation","?")] += 1

    st.markdown("**Library**")
    c1, c2 = st.columns(2)
    c1.metric("ZIPs", n_zips)
    c2.metric("Trained", len(index))
    if index:
        chips = ""
        for op, cnt in sorted(op_cnts.items()):
            cls = {"GET":"chip-get","CREATE":"chip-create",
                   "UPDATE":"chip-update","DELETE":"chip-delete"}.get(op,"chip-get")
            chips += f'<span class="chip {cls}">{op} {cnt}</span>'
        st.markdown(chips, unsafe_allow_html=True)

    st.markdown("---")
    up = ollama_ok()
    if up:
        st.markdown(f'<div class="sbox sbox-ok">● Ollama online<br>'
                    f'<small style="opacity:.7">{OLLAMA_MODEL}</small></div>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<div class="sbox sbox-warn">● Ollama offline<br>'
                    '<small>Python fallback active</small></div>',
                    unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("**Quick prompts**")
    quick = [
        "GET iFlow for Purchase Orders from S/4HANA with Groovy",
        "GET iFlow for Sales Orders",
        "POST iFlow to create Purchase Order with Groovy",
        "PUT iFlow to update Project Elements",
        "DELETE iFlow for A_PurchaseOrder",
        "GET iFlow for A_JournalEntryItemBasic",
        "Generate New Smartapp ONE Package",
    ]
    for q in quick:
        if st.button(q, key=f"q_{q[:20]}", use_container_width=True):
            st.session_state.messages.append({"role":"user","content":q})
            if smartapp_prompt_requested(q):
                st.session_state.pending_cfg = {"smartapp_package": True, "prompt": q}
            else:
                st.session_state.pending_cfg = parse_intent(q)
            st.rerun()

    with st.expander("Smartapp ONE package inputs", expanded=True):
        st.caption("Used when you generate a Smartapp ONE package. Enter the source values to find inside the package and the target values to write into the package.")
        st.text_input("Current Hostname", key="smartapp_old_host", placeholder="e.g. my401471.s4hana.cloud.sap:443")
        st.text_input("New Hostname", key="smartapp_new_host", placeholder="Enter hostname to apply")
        st.text_input("Current Credential", key="smartapp_old_cred", placeholder="e.g. SAPCloud")
        st.text_input("New Credential", key="smartapp_new_cred", placeholder="Enter credential to apply")

    st.markdown("---")
    if st.button("🗑 Clear chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — CHAT
# ═══════════════════════════════════════════════════════════════════════════════

if mode == "💬 Chat & Generate":

    st.markdown("### SAP CPI iFlow Generator")
    st.caption("Describe any iFlow in plain English — GET, POST, PUT, or DELETE. "
               "Upload and train your iFlows first for best template matching.")

    # Render history
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="msg-user"><div class="bubble">{msg["content"]}</div></div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                f'<div class="msg-bot"><div class="avatar">⚡</div>'
                f'<div class="bubble">{msg["content"]}</div></div>',
                unsafe_allow_html=True)
            if msg.get("has_zip") and msg.get("zip_key") and \
                    msg["zip_key"] in st.session_state:
                st.download_button(
                    label     = f"⬇ Download {msg.get('zip_fname','iflow.zip')}",
                    data      = st.session_state[msg["zip_key"]],
                    file_name = msg.get("zip_fname","iflow.zip"),
                    mime      = "application/zip",
                    key       = f"dl_{msg['zip_key']}",
                )

    # Process pending (from quick buttons)
    if st.session_state.pending_cfg is not None:
        cfg = st.session_state.pending_cfg
        st.session_state.pending_cfg = None
        zkey  = f"zip_{int(time.time()*1000)}"

        if cfg.get("smartapp_package"):
            replacements_override = {
                "old_host": st.session_state.get("smartapp_old_host", "").strip(),
                "new_host": st.session_state.get("smartapp_new_host", "").strip(),
                "old_cred": st.session_state.get("smartapp_old_cred", "").strip(),
                "new_cred": st.session_state.get("smartapp_new_cred", "").strip(),
            }
            if not replacements_override["new_host"] or not replacements_override["new_cred"]:
                st.session_state.messages.append({
                    "role":"assistant",
                    "content":"Please enter **New Hostname** and **New Credential** in the sidebar under **Smartapp ONE package inputs**, then run the Smartapp ONE package prompt again."
                })
                st.rerun()
            with st.spinner("Generating Smartapp ONE package…"):
                zip_bytes, summary = generate_smartapp_package(
                    cfg.get("prompt", "Generate New Smartapp ONE Package"),
                    replacements_override=replacements_override,
                )
            fname = "Smartapp_ONE_modified.zip"
            reply = summary
        else:
            index = load_index()
            with st.spinner("Generating your iFlow…"):
                zip_bytes, groovy_code, summary = generate_iflow(cfg, index)
            fname = f"{safe_slug(cfg['iflow_name'])}.zip"
            reply = summary
            if groovy_code:
                reply += f"\n\n**Groovy script:** `{cfg['operation']}` pattern for entity `{cfg['entity_name']}`"

        st.session_state[zkey] = zip_bytes
        st.session_state.messages.append({
            "role":"assistant","content":reply,
            "has_zip":True,"zip_key":zkey,"zip_fname":fname,
        })
        (OUTPUT_DIR / fname).write_bytes(zip_bytes)
        st.rerun()

    # Chat input
    with st.form(key="chat_form", clear_on_submit=True):
        col_inp, col_btn = st.columns([8,1])
        with col_inp:
            user_input = st.text_input(
                "prompt", label_visibility="collapsed",
                placeholder="e.g.  GET iFlow to fetch Purchase Orders from S/4HANA with Groovy",
            )
        with col_btn:
            submitted = st.form_submit_button("Send", use_container_width=True)

    if submitted and user_input.strip():
        prompt = user_input.strip()
        st.session_state.messages.append({"role":"user","content":prompt})

        if smartapp_prompt_requested(prompt):
            replacements_override = {
                "old_host": st.session_state.get("smartapp_old_host", "").strip(),
                "new_host": st.session_state.get("smartapp_new_host", "").strip(),
                "old_cred": st.session_state.get("smartapp_old_cred", "").strip(),
                "new_cred": st.session_state.get("smartapp_new_cred", "").strip(),
            }
            missing_fields = []
            if not replacements_override["new_host"]:
                missing_fields.append("New Hostname")
            if not replacements_override["new_cred"]:
                missing_fields.append("New Credential")
            if missing_fields:
                st.session_state.messages.append({
                    "role":"assistant",
                    "content":"Please fill these sidebar fields before generating the Smartapp ONE package: **" + "**, **".join(missing_fields) + "**."
                })
                st.rerun()
            with st.spinner("Generating Smartapp ONE package…"):
                zip_bytes, summary = generate_smartapp_package(prompt, replacements_override=replacements_override)
            fname = "Smartapp_ONE_modified.zip"
            zkey  = f"zip_{int(time.time()*1000)}"
            st.session_state[zkey] = zip_bytes
            st.session_state.messages.append({
                "role":"assistant","content":summary,
                "has_zip":True,"zip_key":zkey,"zip_fname":fname,
            })
            (OUTPUT_DIR / fname).write_bytes(zip_bytes)
            st.rerun()

        cfg   = parse_intent(prompt)
        index = load_index()
        if not index:
            st.warning("⚠ No trained index found. Go to **🧠 Train Index** first to get accurate template matching. Using fallback template.")

        # Ollama narrative
        thinking = st.empty()
        if ollama_ok():
            llm_prompt = f"""You are an SAP CPI expert. The user wants to build an iFlow.

User: {prompt}
Parsed: Operation={cfg['operation']}, Entity={cfg['entity_name']}, Path={cfg['sender_path']}, Groovy={'yes' if cfg['groovy_needed'] else 'no'}
Trained templates available: {len(index)} (best match operation: {cfg['operation']})

Reply in 2-3 sentences: confirm what you're building, mention if using a real template or skeleton, note Groovy if included."""
            with thinking:
                with st.spinner("Thinking…"):
                    narrative = ollama_stream(llm_prompt)
        else:
            entity_str = cfg['entity_name'] or 'entity'
            narrative = (
                f"Generating a **{cfg['operation']}** iFlow named `{cfg['iflow_name']}`. "
                f"Sender path: `{cfg['sender_path']}` → Entity: `{entity_str}`. "
                + ("Groovy transformation included. " if cfg['groovy_needed'] else "")
                + f"Searching {len(index)} trained templates…"
            )
        thinking.empty()

        with st.spinner("Building ZIP…"):
            zip_bytes, groovy_code, summary = generate_iflow(cfg, index)

        fname = f"{safe_slug(cfg['iflow_name'])}.zip"
        zkey  = f"zip_{int(time.time()*1000)}"
        st.session_state[zkey] = zip_bytes
        full_reply = narrative + "\n\n" + summary
        if groovy_code:
            full_reply += f"\n\n**Groovy script:** `{cfg['operation']}` pattern for `{cfg['entity_name']}`"
        st.session_state.messages.append({
            "role":"assistant","content":full_reply,
            "has_zip":True,"zip_key":zkey,"zip_fname":fname,
        })
        (OUTPUT_DIR / fname).write_bytes(zip_bytes)
        st.rerun()

    if not st.session_state.messages:
        st.markdown("""
<div style="text-align:center;padding:60px 20px;color:#8b949e;">
  <div style="font-size:48px;margin-bottom:16px;">⚡</div>
  <div style="font-size:20px;font-weight:600;color:#e6edf3;margin-bottom:8px;">
    SAP CPI iFlow Intelligence Generator
  </div>
  <div style="font-size:14px;margin-bottom:24px;">
    Describe any iFlow in plain English. Upload your iFlows first for best results.
  </div>
  <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;">
    <span style="background:#21262d;border:1px solid #30363d;border-radius:8px;padding:8px 14px;font-size:13px;">
      💬 "GET iFlow for Purchase Orders with Groovy"
    </span>
    <span style="background:#21262d;border:1px solid #30363d;border-radius:8px;padding:8px 14px;font-size:13px;">
      💬 "POST iFlow to create Sales Order"
    </span>
    <span style="background:#21262d;border:1px solid #30363d;border-radius:8px;padding:8px 14px;font-size:13px;">
      💬 "DELETE iFlow for A_PurchaseOrder"
    </span>
  </div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# UPLOAD
# ═══════════════════════════════════════════════════════════════════════════════

elif mode == "📁 Upload iFlows":
    st.markdown("## 📁 Upload iFlow Template Library")
    st.info(
        "Upload your iFlow ZIPs — supports both **single iFlow ZIPs** and "
        "**SAP CPI package-level exports** (the kind you export from Integration Suite). "
        "Package exports are automatically unwrapped — each iFlow inside becomes a separate template."
    )

    uploaded = st.file_uploader(
        "Drop your iFlow ZIPs here",
        type=["zip"], accept_multiple_files=True,
    )

    if uploaded:
        st.success(f"{len(uploaded)} file(s) selected.")
        if st.button("⬆ Upload to Template Library", type="primary", use_container_width=True):
            counts = defaultdict(int)
            errors = []
            bar    = st.progress(0)
            status = st.empty()
            total_iflows = 0

            for i, f in enumerate(uploaded):
                bar.progress((i+1)/len(uploaded))
                status.text(f"Processing {f.name}…")
                try:
                    zb    = f.read()
                    (PACKAGE_DIR / f.name).write_bytes(zb)
                    flows = process_uploaded_file(zb, f.name)
                    for flow in flows:
                        tid = flow["id"]
                        raw = flow.get("raw_zip_bytes", zb)
                        (TEMPLATE_DIR / f"{tid}.zip").write_bytes(raw)
                        meta = {k:v for k,v in flow.items()
                                if k not in ("xml","groovy_scripts","raw_zip_bytes")}
                        meta["xml_preview"] = flow.get("xml","")[:400]
                        (TEMPLATE_DIR / f"{tid}.meta.json").write_text(
                            json.dumps(meta, indent=2))
                        counts[flow["operation"]] += 1
                        total_iflows += 1
                except Exception as e:
                    errors.append(f"{f.name}: {e}")

            bar.empty(); status.empty()
            st.success(f"✓ {total_iflows} iFlow(s) saved from {len(uploaded)} file(s)")
            cols = st.columns(4)
            for col, op in zip(cols, ["GET","CREATE","UPDATE","DELETE"]):
                col.metric(op, counts[op])
            if errors:
                with st.expander(f"⚠ {len(errors)} error(s)"):
                    for e in errors: st.text(e)
            st.info("👉 Switch to **🧠 Train Index** next.")

    existing = sorted(TEMPLATE_DIR.glob("*.zip"))
    if existing:
        st.markdown(f"---\n**Library: {len(existing)} template ZIP(s)**")
        rows = []
        for z in existing[:100]:
            mp = TEMPLATE_DIR / f"{z.stem}.meta.json"
            meta = {}
            if mp.exists():
                try: meta = json.loads(mp.read_text())
                except: pass
            rows.append({
                "Name":        meta.get("name", z.stem),
                "Operation":   meta.get("operation","?"),
                "Entity":      (meta.get("props") or {}).get("entity_name",""),
                "Sender Path": (meta.get("props") or {}).get("sender_path",""),
                "Groovy":      "Yes" if meta.get("groovy_files") else "No",
                "Size (KB)":   round(z.stat().st_size/1024, 1),
            })
        if rows:
            import pandas as pd
            st.dataframe(pd.DataFrame(rows), use_container_width=True, height=400)


# ═══════════════════════════════════════════════════════════════════════════════
# TRAIN
# ═══════════════════════════════════════════════════════════════════════════════

elif mode == "🧠 Train Index":
    st.markdown("## 🧠 Train — Build Pattern Index")
    st.info(
        "Reads every ZIP in the template library, extracts all patterns "
        "(adapter types, sender paths, entity names, OData URLs, Groovy scripts, mappings), "
        "and writes the searchable index. Re-run whenever you add new ZIPs."
    )

    zips = list(TEMPLATE_DIR.glob("*.zip"))
    if not zips:
        st.warning("No ZIPs in template_library/. Upload first.")
        st.stop()

    st.metric("ZIPs ready to train", len(zips))

    if st.button("🧠 Train All iFlows Now", type="primary", use_container_width=True):
        records, errors = [], []
        op_cnts = defaultdict(int)
        groovy_cnt = mapping_cnt = 0
        bar    = st.progress(0)
        status = st.empty()
        log    = st.expander("Training log", expanded=True)

        for i, zpath in enumerate(zips):
            bar.progress((i+1)/len(zips))
            status.text(f"[{i+1}/{len(zips)}] {zpath.name}")
            try:
                zb  = zpath.read_bytes()
                rec = parse_iflow_zip(zb, zpath.name)
                rec["id"] = zpath.stem
                records.append(rec)
                op_cnts[rec["operation"]] += 1
                if rec["groovy_scripts"]: groovy_cnt  += 1
                if rec["has_mapping"]:    mapping_cnt += 1
                p = rec.get("props",{})
                with log:
                    st.markdown(
                        f"`{rec['operation']}` **{rec['name']}** — "
                        f"entity:`{p.get('entity_name','–')}` "
                        f"path:`{p.get('sender_path','–')}` "
                        f"groovy:{len(rec['groovy_scripts'])}"
                    )
            except Exception as e:
                errors.append(f"{zpath.name}: {e}")

        save_index(records)
        bar.empty(); status.empty()
        st.success(f"✓ Training complete — {len(records)} iFlows indexed")

        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Total",    len(records))
        c2.metric("GET",      op_cnts["GET"])
        c3.metric("CREATE",   op_cnts["CREATE"])
        c4.metric("UPDATE",   op_cnts["UPDATE"])
        c5.metric("DELETE",   op_cnts["DELETE"])
        c6.metric("w/Groovy", groovy_cnt)

        if errors:
            with st.expander(f"⚠ {len(errors)} error(s)"):
                for e in errors: st.text(e)

        st.success("👉 Switch to **💬 Chat & Generate**!")

    if INDEX_FILE.exists():
        idx = load_index()
        st.markdown(f"---\n**Current index:** {len(idx)} records")
        groovy_all = []
        for rec in idx:
            for s in rec.get("groovy_scripts",[]):
                groovy_all.append({
                    "iFlow":     rec["name"],
                    "Operation": rec["operation"],
                    "Script":    Path(s["file"]).name,
                })
        if groovy_all:
            st.markdown(f"**{len(groovy_all)} Groovy scripts extracted:**")
            import pandas as pd
            st.dataframe(pd.DataFrame(groovy_all), use_container_width=True, height=250)
        if st.button("🗑 Clear index"):
            INDEX_FILE.unlink()
            st.rerun()
