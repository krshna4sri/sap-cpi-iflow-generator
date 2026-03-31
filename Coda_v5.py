"""
SAP Intelligence Suite — v4.0
════════════════════════════════════════════════════════════
v4.0 adds three new capability pillars on top of v3.2:

  NEW ─ Intelligent ABAP & RAP Code Generation
        • ABAP Programs (SE38-ready)
        • CDS Views (Eclipse ADT-ready)
        • RAP Models (Root View + BDEF + Implementation + Service)
        • OData Services (via RAP Service Binding)
        • Class-based ABAP (SE24/ADT)
        • Financial Commitment Pooling (PO + FM/Budget — EKKO/EKPO/FMIFIIT/RPSCO)

  NEW ─ Functional + Configuration Knowledge
        • Process Order, Production Order, Internal Order
        • Financial Commitment & Funds Management
        • Procure to Pay, Order to Cash, Plan to Produce
        • Asset Accounting, Batch Management, Profit Center
        • MRP, Warehouse Management, SAP Activate
        • RAP/CDS concepts and configuration guidance

  PRESERVED ─ v3.2 CPI iFlow Generation (all fixes intact)
        • MANIFEST.MF exact SAP CPI format
        • SAP-BundleType: IntegrationFlow
        • Full Import-Package OSGi block
        • Groovy pattern library
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
DOCS_DIR     = Path("docs_library")
DOCS_INDEX   = Path("trained_index/docs_index.json")

for d in [TEMPLATE_DIR, INDEX_FILE.parent, OUTPUT_DIR, PACKAGE_DIR, DOCS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")

TEXT_SUFFIXES = {
    ".iflw", ".groovy", ".prop", ".propdef", ".mf",
    ".edmx", ".xsd", ".wsdl", ".mmap", ".project", ".xml"
}

# ═══════════════════════════════════════════════════════════════════════════════
# CORRECT MANIFEST.MF  (v4.0 fix — proper OSGi 72-byte line wrapping)
# Verified against real SAP CPI package exports.
# Critical fields CPI parser requires:
#   • SAP-BundleType: IntegrationFlow  (NOT "IFlow")
#   • Import-Package: full OSGi package list (CPI won't load without this)
#   • SAP-RuntimeProfile: iflmap
#   • SAP-NodeType: IFLMAP
#   • \r\n line endings throughout (CRLF — Java OSGi parser requirement)
#   • Continuation lines start with exactly one space
#   • EACH LINE must be ≤ 72 bytes of content (Java ManifestInputStream limit)
#     Lines at exactly 72 bytes are rejected by some JVM versions.
#     Safe limit: 70 bytes of content + \r\n = 72 bytes total.
#   • File must end with \r\n\r\n (blank line)
# ═══════════════════════════════════════════════════════════════════════════════

def _wrap_mf_header(name: str, value: str, max_content: int = 70) -> bytes:
    """
    Wrap a manifest header value per OSGi JAR spec section 3.3.
    Java's ManifestInputStream treats max line = 72 bytes of content (not including CRLF).
    We use 70 to stay safely below the limit on all JVM implementations.

    First line : 'Header-Name: <value_start>'  (total content <= max_content)
    Continuation: ' <value_cont>'               (total content <= max_content, 1-byte space prefix)
    All lines end with \r\n.
    """
    prefix    = (name + ": ").encode("utf-8")
    val_bytes = value.encode("utf-8")
    result    = []

    first_max = max_content - len(prefix)
    if first_max <= 0:
        raise ValueError(f"Header name '{name}' too long for max_content={max_content}")

    result.append(prefix + val_bytes[:first_max] + b"\r\n")
    pos = first_max
    cont_max = max_content - 1   # 1 byte for the leading space

    while pos < len(val_bytes):
        result.append(b" " + val_bytes[pos:pos + cont_max] + b"\r\n")
        pos += cont_max

    return b"".join(result)


# Import-Package and Import-Service values — identical across all iFlows
_IMPORT_PKG_VALUE = (
    "com.sap.esb.application.services.cxf.interceptor,"
    "com.sap.esb.security,"
    "com.sap.it.op.agent.api,"
    "com.sap.it.op.agent.collector.camel,"
    "com.sap.it.op.agent.collector.cxf,"
    "com.sap.it.op.agent.mpl,"
    "javax.jms,javax.jws,javax.wsdl,"
    "javax.xml.bind.annotation,javax.xml.namespace,javax.xml.ws,"
    "org.apache.camel;version=\"2.8\"," 
    "org.apache.camel.builder;version=\"2.8\"," 
    "org.apache.camel.builder.xml;version=\"2.8\"," 
    "org.apache.camel.component.cxf,"
    "org.apache.camel.model;version=\"2.8\"," 
    "org.apache.camel.processor;version=\"2.8\"," 
    "org.apache.camel.processor.aggregate;version=\"2.8\"," 
    "org.apache.camel.spring.spi;version=\"2.8\"," 
    "org.apache.commons.logging,"
    "org.apache.cxf.binding,"
    "org.apache.cxf.binding.soap,"
    "org.apache.cxf.binding.soap.spring,"
    "org.apache.cxf.bus,org.apache.cxf.bus.resource,org.apache.cxf.bus.spring,"
    "org.apache.cxf.buslifecycle,org.apache.cxf.catalog,"
    "org.apache.cxf.configuration.jsse;version=\"2.5\"," 
    "org.apache.cxf.configuration.spring,"
    "org.apache.cxf.endpoint,org.apache.cxf.headers,org.apache.cxf.interceptor,"
    "org.apache.cxf.management.counters;version=\"2.5\"," 
    "org.apache.cxf.message,org.apache.cxf.phase,org.apache.cxf.resource,"
    "org.apache.cxf.service.factory,org.apache.cxf.service.model,"
    "org.apache.cxf.transport,"
    "org.apache.cxf.transport.common.gzip,org.apache.cxf.transport.http,"
    "org.apache.cxf.transport.http.policy,org.apache.cxf.workqueue,"
    "org.apache.cxf.ws.rm.persistence,org.apache.cxf.wsdl11,"
    "org.osgi.framework;version=\"1.6.0\"," 
    "org.slf4j;version=\"1.6\"," 
    "org.springframework.beans.factory.config;version=\"3.0\"," 
    "com.sap.esb.camel.security.cms,"
    "org.apache.camel.spi,"
    "com.sap.esb.webservice.audit.log,"
    "com.sap.esb.camel.endpoint.configurator.api,"
    "com.sap.esb.camel.jdbc.idempotency.reorg,"
    "javax.sql,"
    "org.apache.camel.processor.idempotent.jdbc,"
    "org.osgi.service.blueprint;version=\"[1.0.0,2.0.0)\""
)

_IMPORT_SVC_VALUE = (
    "com.sap.esb.webservice.audit.log.AuditLogger,"
    "com.sap.esb.security.KeyManagerFactory;multiple:=false,"
    "com.sap.esb.security.TrustManagerFactory;multiple:=false,"
    "javax.sql.DataSource;multiple:=false;filter=\"(dataSourceName=default)\"," 
    "org.apache.cxf.ws.rm.persistence.RMStore;multiple:=false,"
    "com.sap.esb.camel.security.cms.SignatureSplitter;multiple:=false"
)


def make_manifest(artifact_id: str, iflow_name: str) -> bytes:
    """
    Build a MANIFEST.MF that SAP CPI accepts and auto-populates the upload dialog.

    CPI auto-fill behaviour (verified from real exports):
      ID field   <- Bundle-SymbolicName  (underscore_safe, no spaces)
      Name field <- Bundle-Name          (human readable, spaces OK)

    v4.0: Uses _wrap_mf_header() to correctly wrap at 70 bytes of content per line,
    satisfying Java's ManifestInputStream which rejects lines > 72 bytes.
    """
    display_name = artifact_id.replace("_", " ")

    parts = [
        f"Manifest-Version: 1.0\r\n".encode(),
        f"Bundle-SymbolicName: {artifact_id}\r\n".encode(),
        f"Bundle-ManifestVersion: 2\r\n".encode(),
        f"Origin-Bundle-SymbolicName: {artifact_id}\r\n".encode(),
        f"SAP-ArtifactTrait: \r\n".encode(),
        _wrap_mf_header("Import-Package", _IMPORT_PKG_VALUE),
        f"Origin-Bundle-Name: {display_name}\r\n".encode(),
        f"SAP-RuntimeProfile: iflmap\r\n".encode(),
        f"Bundle-Name: {display_name}\r\n".encode(),
        f"Bundle-Version: 1.0.0\r\n".encode(),
        f"SAP-NodeType: IFLMAP\r\n".encode(),
        f"SAP-BundleType: IntegrationFlow\r\n".encode(),
        _wrap_mf_header("Import-Service", _IMPORT_SVC_VALUE),
        f"Origin-Bundle-Version: 1.0.0\r\n".encode(),
        b"\r\n",
    ]
    return b"".join(parts)


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
    """
    Generate Eclipse .project file.
    CRITICAL: Must use <name> tag (not <n>) — SAP CPI and Eclipse both
    require the standard Eclipse projectDescription/name element.
    Wrong tag = CPI cannot identify project root = 'valid source folders' error.
    """
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<projectDescription>
	<name>{artifact_id}</name>
	<comment/>
	<projects/>
	<buildSpec>
		<buildCommand>
			<name>org.eclipse.jdt.core.javabuilder</name>
			<arguments/>
		</buildCommand>
	</buildSpec>
	<natures>
		<nature>org.eclipse.jdt.core.javanature</nature>
		<nature>com.sap.ide.ifl.project.support.project.nature</nature>
		<nature>com.sap.ide.ifl.bsn</nature>
	</natures>
</projectDescription>
'''.encode("utf-8")





def make_parameters_propdef() -> bytes:
    """
    parameters.propdef — exact format from real SAP CPI iFlow exports.
    Verified from Smartapp exported iFlow ZIP.
    """
    return b'''<?xml version="1.0" encoding="UTF-8" standalone="no"?><parameters><param_references/></parameters>'''


def make_parameters_prop() -> bytes:
    """
    parameters.prop — minimal Java properties file.
    Real CPI exports contain just a timestamp comment line.
    """
    import time
    ts = time.strftime("#%a %b %d %H:%M:%S UTC %Y")
    return (ts + "\n").encode("utf-8")


def make_metainfo_prop(iflow_name: str, source: str = "SAP Intelligence Suite",
                       target: str = "S4HANA Cloud") -> bytes:
    """
    metainfo.prop — present in real CPI iFlow exports.
    Stores descriptive metadata about the iFlow.
    """
    import time
    ts = time.strftime("#%a %b %d %H:%M:%S UTC %Y")
    display = iflow_name.replace("_", " ")
    return (
        f"#Store metainfo properties\n"
        f"{ts}\n"
        f"description={display}\n"
        f"source={source}\n"
        f"target={target}\n"
    ).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN SKELETON iFlow XMLs (fallback only — real templates preferred)
# ═══════════════════════════════════════════════════════════════════════════════

def _skeleton(method: str, odata_op: str) -> str:
    """
    CPI-compatible iFlw XML skeleton — verified against real Smartapp iFlow exports.
    Key fixes vs original:
      - xmlns:di namespace added
      - Collaboration extensionElements with cmdVariantUri / componentVersion
      - Participants use ifl:type attribute
      - Process has transactionTimeout + cmdVariantUri
      - Steps use activityType + cmdVariantUri (not ComponentType)
      - bpmndi:BPMNDiagram layout section included (CPI editor requires it to open)
    """
    xml  = '<?xml version="1.0" encoding="UTF-8"?>'
    xml += '<bpmn2:definitions'
    xml += ' xmlns:bpmn2="http://www.omg.org/spec/BPMN/20100524/MODEL"'
    xml += ' xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"'
    xml += ' xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"'
    xml += ' xmlns:di="http://www.omg.org/spec/DD/20100524/DI"'
    xml += ' xmlns:ifl="http:///com.sap.ifl.model/Ifl.xsd"'
    xml += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    xml += ' id="Definitions_1">'

    # ── Collaboration ───────────────────────────────────────────────────────────
    xml += '<bpmn2:collaboration id="Collaboration_1" name="Default Collaboration">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>namespaceMapping</key><value/></ifl:property>'
    xml += '<ifl:property><key>httpSessionHandling</key><value>None</value></ifl:property>'
    xml += '<ifl:property><key>returnExceptionToSender</key><value>false</value></ifl:property>'
    xml += '<ifl:property><key>log</key><value>All events</value></ifl:property>'
    xml += '<ifl:property><key>componentVersion</key><value>1.2</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::IFlowVariant/cname::IFlowConfiguration/version::1.2.2</value>'
    xml += '</ifl:property>'
    xml += '</bpmn2:extensionElements>'

    # Sender participant
    xml += '<bpmn2:participant id="Participant_1" ifl:type="EndpointSender" name="Sender">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>enableBasicAuthentication</key><value>false</value></ifl:property>'
    xml += '<ifl:property><key>ifl:type</key><value>EndpointSender</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '</bpmn2:participant>'

    # Receiver participant
    xml += '<bpmn2:participant id="Participant_2" ifl:type="EndpointRecevier" name="Receiver">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>ifl:type</key><value>EndpointRecevier</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '</bpmn2:participant>'

    # Integration Process participant
    xml += '<bpmn2:participant id="Participant_Process_1" ifl:type="IntegrationProcess"'
    xml += ' name="Integration Process" processRef="Process_1">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>ifl:type</key><value>IntegrationProcess</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '</bpmn2:participant>'

    # Message flows
    xml += '<bpmn2:messageFlow id="MessageFlow_1" name="%%IFLOW_NAME%%"'
    xml += ' sourceRef="Participant_1" targetRef="StartEvent_1"/>'
    xml += '<bpmn2:messageFlow id="MessageFlow_2" name="%%IFLOW_NAME%%"'
    xml += ' sourceRef="EndEvent_1" targetRef="Participant_2"/>'
    xml += '</bpmn2:collaboration>'

    # ── Integration Process ─────────────────────────────────────────────────────
    xml += '<bpmn2:process id="Process_1" name="Integration Process">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>transactionTimeout</key><value>30</value></ifl:property>'
    xml += '<ifl:property><key>componentVersion</key><value>1.1</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::FlowElementVariant/cname::IntegrationProcess/version::1.1.3</value>'
    xml += '</ifl:property>'
    xml += '<ifl:property><key>transactionalHandling</key><value>Required</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'

    # Start Event
    xml += '<bpmn2:startEvent id="StartEvent_1" name="Start">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>StartEvent</value></ifl:property>'
    xml += '<ifl:property><key>address</key><value>%%SENDER_PATH%%</value></ifl:property>'
    xml += f'<ifl:property><key>httpMethod</key><value>{method}</value></ifl:property>'
    xml += '<ifl:property><key>enableBasicAuthentication</key><value>false</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::FlowstepVariant/cname::StartEvent</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:outgoing>SequenceFlow_1</bpmn2:outgoing>'
    xml += '</bpmn2:startEvent>'

    # Content Modifier — set Accept header
    xml += '<bpmn2:callActivity id="CallActivity_1" name="Set Headers">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>ContentModifier</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::FlowstepVariant/cname::ContentModifier</value></ifl:property>'
    xml += '<ifl:property><key>messageHeaderTable</key>'
    xml += f'<value>Accept=application/json&#xA;CamelHttpMethod={method}</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SequenceFlow_1</bpmn2:incoming>'
    xml += '<bpmn2:outgoing>SequenceFlow_2</bpmn2:outgoing>'
    xml += '</bpmn2:callActivity>'

    # OData Receiver
    xml += f'<bpmn2:callActivity id="CallActivity_2" name="{odata_op} %%ENTITY_NAME%%">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>ODataV2Receiver</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::FlowstepVariant/cname::ODataV2Receiver</value></ifl:property>'
    xml += '<ifl:property><key>address</key><value>%%ODATA_ADDRESS%%</value></ifl:property>'
    xml += '<ifl:property><key>entitySetName</key><value>%%ENTITY_NAME%%</value></ifl:property>'
    xml += f'<ifl:property><key>operation</key><value>{odata_op}</value></ifl:property>'
    xml += '<ifl:property><key>authenticationMethod</key><value>BasicAuthentication</value></ifl:property>'
    xml += '<ifl:property><key>credentialName</key><value>S4HANA_CRED</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SequenceFlow_2</bpmn2:incoming>'
    xml += '<bpmn2:outgoing>SequenceFlow_3</bpmn2:outgoing>'
    xml += '</bpmn2:callActivity>'

    # End Event
    xml += '<bpmn2:endEvent id="EndEvent_1" name="End">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>MessageEndEvent</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key>'
    xml += '<value>ctype::FlowstepVariant/cname::MessageEndEvent</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SequenceFlow_3</bpmn2:incoming>'
    xml += '<bpmn2:messageEventDefinition/>'
    xml += '</bpmn2:endEvent>'

    # Sequence flows
    xml += '<bpmn2:sequenceFlow id="SequenceFlow_1" sourceRef="StartEvent_1" targetRef="CallActivity_1"/>'
    xml += '<bpmn2:sequenceFlow id="SequenceFlow_2" sourceRef="CallActivity_1" targetRef="CallActivity_2"/>'
    xml += '<bpmn2:sequenceFlow id="SequenceFlow_3" sourceRef="CallActivity_2" targetRef="EndEvent_1"/>'
    xml += '</bpmn2:process>'

    # ── BPMN Diagram (visual layout — required by CPI editor to open) ──────────
    xml += '<bpmndi:BPMNDiagram id="BPMNDiagram_1" name="Default Collaboration Diagram">'
    xml += '<bpmndi:BPMNPlane bpmnElement="Collaboration_1" id="BPMNPlane_1">'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_1" id="BPMNShape_P1" isHorizontal="true">'
    xml += '<dc:Bounds height="200.0" width="100.0" x="30.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_Process_1" id="BPMNShape_PP1" isHorizontal="true">'
    xml += '<dc:Bounds height="200.0" width="760.0" x="130.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_2" id="BPMNShape_P2" isHorizontal="true">'
    xml += '<dc:Bounds height="200.0" width="100.0" x="890.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="StartEvent_1" id="BPMNShape_SE1">'
    xml += '<dc:Bounds height="32.0" width="32.0" x="190.0" y="134.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="CallActivity_1" id="BPMNShape_CA1">'
    xml += '<dc:Bounds height="60.0" width="100.0" x="280.0" y="120.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="CallActivity_2" id="BPMNShape_CA2">'
    xml += '<dc:Bounds height="60.0" width="100.0" x="460.0" y="120.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="EndEvent_1" id="BPMNShape_EE1">'
    xml += '<dc:Bounds height="32.0" width="32.0" x="640.0" y="134.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNEdge bpmnElement="MessageFlow_1" id="BPMNEdge_MF1">'
    xml += '<di:waypoint x="130.0" y="150.0"/><di:waypoint x="190.0" y="150.0"/>'
    xml += '</bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="MessageFlow_2" id="BPMNEdge_MF2">'
    xml += '<di:waypoint x="672.0" y="150.0"/><di:waypoint x="890.0" y="150.0"/>'
    xml += '</bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SequenceFlow_1" id="BPMNEdge_SF1">'
    xml += '<di:waypoint x="222.0" y="150.0"/><di:waypoint x="280.0" y="150.0"/>'
    xml += '</bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SequenceFlow_2" id="BPMNEdge_SF2">'
    xml += '<di:waypoint x="380.0" y="150.0"/><di:waypoint x="460.0" y="150.0"/>'
    xml += '</bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SequenceFlow_3" id="BPMNEdge_SF3">'
    xml += '<di:waypoint x="560.0" y="150.0"/><di:waypoint x="640.0" y="150.0"/>'
    xml += '</bpmndi:BPMNEdge>'
    xml += '</bpmndi:BPMNPlane>'
    xml += '</bpmndi:BPMNDiagram>'
    xml += '</bpmn2:definitions>'
    return xml


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
                    sender_adapter: str = "HTTPS",
                    preferred_template: str = "") -> Optional[Dict]:
    """
    Score each template and return the best match.
    Scoring:
      +50  template name contains preferred_template string (user-pinned — dominates)
      +10  operation matches (GET/CREATE/UPDATE/DELETE)
      +8   sender adapter matches (HTTPS vs ProcessDirect etc)
      +5   entity name contains hint
      +3   sender path contains hint
      +2   iFlow name contains operation keyword

    Only returns a match if score > 0 (operation must match).
    preferred_template gives a +50 bonus so it always wins unless
    the operation doesn't match at all.
    """
    op = operation.upper()
    best = (0, None)
    pref_lower = preferred_template.strip().lower() if preferred_template else ""

    for rec in index:
        score = 0
        rec_op = rec.get("operation","").upper()

        # Operation match is mandatory
        if rec_op == op:
            score += 10
        else:
            continue  # skip templates with wrong operation entirely

        rec_name = rec.get("name","").lower()

        # ── Preferred template bonus — user-pinned, dominates all other scoring ──
        if pref_lower and pref_lower in rec_name:
            score += 50   # ensures this template wins decisively

        # Adapter type match
        rec_adapter = rec.get("props",{}).get("sender_adapter","HTTPS")
        if sender_adapter and rec_adapter.upper() == sender_adapter.upper():
            score += 8

        # Entity name similarity
        rec_entity = rec.get("props",{}).get("entity_name","").lower()
        if entity_hint and entity_hint.lower() in rec_entity:
            score += 5
        elif entity_hint and rec_entity and rec_entity in entity_hint.lower():
            score += 2

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
    """
    Build a fresh CPI-importable ZIP from a skeleton XML.

    JAR/OSGi spec requirements for SAP CPI:
      1. META-INF/MANIFEST.MF must be the FIRST entry in the ZIP
      2. META-INF/MANIFEST.MF must be UNCOMPRESSED (ZIP_STORED, compress_type=0)
         — SAP CPI reads the manifest before decompression; if compressed, the
           parser cannot read it → blank Name/ID fields → "invalid manifest" error
      3. All other files may use ZIP_DEFLATED for size efficiency
    """
    slug = safe_slug(iflow_name)
    out  = io.BytesIO()
    mf_bytes = make_manifest(artifact_id, iflow_name)

    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        # ── MANIFEST.MF: FIRST entry, UNCOMPRESSED (ZIP_STORED) ──────────
        mf_info              = zipfile.ZipInfo("META-INF/MANIFEST.MF")
        mf_info.compress_type = zipfile.ZIP_STORED
        zf.writestr(mf_info, mf_bytes)

        # ── All other files: compressed normally ──────────────────────────
        zf.writestr(".project",
                    make_project(artifact_id))
        # parameters.prop + parameters.propdef — required by CPI project validator
        zf.writestr("src/main/resources/parameters.prop",
                    make_parameters_prop())
        zf.writestr("src/main/resources/parameters.propdef",
                    make_parameters_propdef())
        # metainfo.prop — present in all real CPI iFlow exports
        zf.writestr("metainfo.prop",
                    make_metainfo_prop(iflow_name))
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
        # Match both <n> (correct) and <n> (legacy wrong) tags
        proj = re.sub(r"<n>[^<]+</n>", f"<n>{artifact_id}</n>", proj)
        proj = re.sub(r"<n>[^<]+</n>", f"<n>{artifact_id}</n>", proj)
        return proj.encode("utf-8")

    # ── Guard: detect package-wrapper ZIPs (no .iflw = unusable as template) ──
    # Package exports contain a nested .zip but no direct .iflw file.
    # Cloning them produces a 1800KB+ corrupt ZIP. Detect and fall back to skeleton.
    try:
        with zipfile.ZipFile(io.BytesIO(original_zip), "r") as _chk:
            _names = _chk.namelist()
            _has_iflw = any(n.endswith(".iflw") for n in _names)
            if not _has_iflw:
                # This is a package-level export wrapper — cannot clone it
                raise ValueError("PACKAGE_WRAPPER: no .iflw found in template ZIP")
    except ValueError as _ve:
        if "PACKAGE_WRAPPER" in str(_ve):
            # Fall through to skeleton build — caller handles this
            raise
        pass  # Other zipfile errors — let the main block handle them

    with zipfile.ZipFile(io.BytesIO(original_zip), "r") as zin:
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:

            # ── Pass 1: Write MANIFEST.MF FIRST and UNCOMPRESSED ─────────
            mf_data = None
            if "META-INF/MANIFEST.MF" in zin.namelist():
                mf_data = _patch_manifest(zin.read("META-INF/MANIFEST.MF"))
            else:
                mf_data = make_manifest(artifact_id, artifact_id)
            mf_info              = zipfile.ZipInfo("META-INF/MANIFEST.MF")
            mf_info.compress_type = zipfile.ZIP_STORED
            zout.writestr(mf_info, mf_data)

            # ── Pass 2: Write all other files ────────────────────────────
            for item in zin.infolist():
                data  = zin.read(item.filename)
                fname = item.filename
                sfx   = Path(fname).suffix.lower()

                # Skip MANIFEST — already written above
                if fname == "META-INF/MANIFEST.MF":
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

            # Ensure required files exist (may be absent in older templates)
            existing_names = set(zin.namelist())
            if "src/main/resources/parameters.prop" not in existing_names:
                zout.writestr("src/main/resources/parameters.prop",
                              make_parameters_prop())
            if "src/main/resources/parameters.propdef" not in existing_names:
                zout.writestr("src/main/resources/parameters.propdef",
                              make_parameters_propdef())
            if "metainfo.prop" not in existing_names:
                zout.writestr("metainfo.prop",
                              make_metainfo_prop(new_name))

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


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONAL DOCS — Train & Search
# ═══════════════════════════════════════════════════════════════════════════════

def _read_txt(raw: bytes) -> str:
    """Decode bytes to text, trying common encodings."""
    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def _read_pdf(raw: bytes) -> str:
    """Extract text from PDF bytes using pypdf (if installed) or fallback."""
    try:
        import pypdf, io as _io
        reader = pypdf.PdfReader(_io.BytesIO(raw))
        pages = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                pass
        return "\n".join(pages)
    except ImportError:
        pass
    try:
        # fallback: try pdfminer
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        import io as _io
        out = _io.StringIO()
        extract_text_to_fp(_io.BytesIO(raw), out, laparams=LAParams())
        return out.getvalue()
    except Exception:
        return ""


def _read_docx(raw: bytes) -> str:
    """Extract text from .docx bytes using python-docx (if installed)."""
    try:
        import docx as _docx, io as _io
        doc = _docx.Document(_io.BytesIO(raw))
        return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    except Exception:
        return ""


def extract_doc_text(filename: str, raw: bytes) -> str:
    """Route to correct extractor based on file extension."""
    ext = Path(filename).suffix.lower()
    if ext == ".pdf":
        return _read_pdf(raw)
    elif ext in (".docx", ".doc"):
        return _read_docx(raw)
    elif ext in (".txt", ".md", ".csv", ".xml", ".json"):
        return _read_txt(raw)
    else:
        return _read_txt(raw)


def chunk_text(text: str, chunk_size: int = 400, overlap: int = 80) -> List[str]:
    """
    Split text into overlapping word-chunks for indexing.
    Each chunk ~ chunk_size words with overlap words from previous chunk.
    """
    words = text.split()
    if not words:
        return []
    chunks = []
    start  = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunks.append(" ".join(words[start:end]))
        if end == len(words):
            break
        start += (chunk_size - overlap)
    return chunks


def load_docs_index() -> List[Dict]:
    """Load the functional docs index from disk."""
    if DOCS_INDEX.exists():
        try:
            return json.loads(DOCS_INDEX.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def save_docs_index(records: List[Dict]) -> None:
    """Persist docs index to disk."""
    DOCS_INDEX.write_text(json.dumps(records, indent=2, ensure_ascii=False),
                          encoding="utf-8")


def train_docs(files: List[Dict]) -> Tuple[int, int, List[str]]:
    """
    Index a list of {name, bytes} dicts.
    Returns (chunks_added, files_ok, errors).
    Each chunk stored as:
      {source, chunk_id, text, keywords}
    """
    existing  = load_docs_index()
    # Remove old records for files being re-trained
    new_names = {f["name"] for f in files}
    kept      = [r for r in existing if r.get("source") not in new_names]

    chunks_added = 0
    errors       = []

    for fdict in files:
        name = fdict["name"]
        raw  = fdict["bytes"]
        try:
            text = extract_doc_text(name, raw)
            if not text.strip():
                errors.append(f"{name}: no text extracted")
                continue
            chunks = chunk_text(text)
            for i, chunk in enumerate(chunks):
                # Simple keyword extraction: unique non-stop words > 3 chars
                words    = re.findall(r'[A-Za-z][A-Za-z0-9_]{2,}', chunk)
                stopwords = {
                    "the","and","for","that","this","with","from","are","not",
                    "its","can","has","have","been","will","you","your","our",
                    "was","were","they","them","their","also","such","into",
                    "which","when","where","what","how","all","any","each",
                    "both","more","than","then","just","about","over","after",
                    "sap","data","value","field","object","type","process",
                }
                kws = list({w.lower() for w in words
                            if w.lower() not in stopwords})[:40]
                kept.append({
                    "source":   name,
                    "chunk_id": i,
                    "text":     chunk,
                    "keywords": kws,
                })
                chunks_added += 1
        except Exception as e:
            errors.append(f"{name}: {e}")

    save_docs_index(kept)
    return chunks_added, len(files) - len(errors), errors


# ── Junk keywords — anything containing these phrases is boilerplate ─────────
# ── Answer Engine ──────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN SAP KNOWLEDGE BASE
# Covers every functional question listed in the Prompt Guide PDF.
# No internet, no trained docs needed — always works.
# ═══════════════════════════════════════════════════════════════════════════════

SAP_KB = {

    # ── SAP SD — Sales & Distribution ─────────────────────────────────────────
    "what is a sales order": (
        "A Sales Order in SAP SD is a document that records a customer's request to purchase "
        "goods or services. Created via transaction VA01. It contains header data (sold-to party, "
        "order date, requested delivery date, payment terms) and line items (material, quantity, "
        "price). Sales Orders are stored in VBAK (header) and VBAP (items). The Sales Order "
        "drives the delivery, goods issue, and billing process."
    ),
    "sales order process": (
        "The SAP SD Sales Order process: 1) Inquiry (VA11) — customer enquiry. "
        "2) Quotation (VA21) — price offer to customer. 3) Sales Order (VA01) — confirmed order. "
        "4) Delivery (VL01N) — pick, pack and goods issue. 5) Billing (VF01) — invoice to customer. "
        "6) Payment — customer pays, recorded in FI. Each step creates a document linked by "
        "document flow, visible in VA03 → Environment → Document Flow."
    ),
    "key components of sap sd": (
        "The key components of SAP Sales and Distribution (SD): "
        "1) Master Data — Customer Master (XD01), Material Master (MM01), Pricing Conditions (VK11). "
        "2) Pre-Sales — Inquiries and Quotations. 3) Sales Order Processing — VA01/VA02/VA03. "
        "4) Shipping — Outbound Delivery (VL01N), Goods Issue (VL02N). "
        "5) Billing — Billing Document (VF01), Credit/Debit Memos. "
        "6) Integration — with MM for stock, FI for accounting, PP for production."
    ),
    "billing document": (
        "A Billing Document in SAP SD represents the invoice sent to the customer after goods "
        "issue. Created via VF01, displayed in VF03. Stored in VBRK (header) and VBRP (items). "
        "The billing document automatically creates an FI accounting document posting to the "
        "customer receivables account. Billing types include F2 (standard invoice), G2 (credit memo) "
        "and L2 (debit memo). Cancellation billing via VF11."
    ),
    "delivery from sales order": (
        "An Outbound Delivery in SAP SD is created from a Sales Order using VL01N or automatically "
        "via VL10. It triggers the warehouse processes: picking (LT0A), packing (VL02N), and "
        "Goods Issue (PGI). PGI reduces stock in MM, creates the accounting document in FI, and "
        "enables billing. Deliveries are stored in LIKP (header) and LIPS (items). "
        "Multiple Sales Orders can be combined into one delivery."
    ),
    "copy control": (
        "Copy Control in SAP SD defines how data flows between documents — e.g., Sales Order to "
        "Delivery, or Delivery to Billing. Configured via VTLA (order to delivery), VTFL (delivery "
        "to billing), VTAA (order to order). Controls which fields are copied, how pricing is "
        "re-determined, and whether quantities transfer fully or partially. "
        "Each combination of source and target document types has its own copy control routine."
    ),
    "item categories in sales orders": (
        "Item Categories in SAP SD control line item behaviour in sales documents. They determine "
        "relevance for delivery (LVPOS), billing (FKREL), pricing, and MRP transfer. "
        "Standard item category TAN is for normal items; TAD for services; TANN for free goods. "
        "Configured in VOV7. Determined by: Sales Document Type + Item Category Group (from "
        "material master) + Usage + Higher-level Item Category."
    ),
    "partner function": (
        "Partner Functions in SAP SD define business partner roles in sales transactions. "
        "Key functions: SP (Sold-to Party) — places the order, SH (Ship-to Party) — receives goods, "
        "BP (Bill-to Party) — receives invoice, PY (Payer) — pays the invoice. "
        "Configured in VOPA and assigned to customer account groups. A customer can have "
        "different partners for each function, e.g., one address for billing, another for delivery."
    ),
    "pricing in sap sd": (
        "Pricing in SAP SD uses the Condition Technique. A Pricing Procedure (configured in OVKK) "
        "defines the sequence of Condition Types. Condition Types represent price elements: "
        "PR00 = base price, K004 = material discount, MWST = output tax. "
        "Condition Records (VK11) store actual values per customer/material combination. "
        "Access Sequences define the search strategy for finding condition records. "
        "The pricing procedure is determined by Sales Area + Customer Pricing Procedure + "
        "Document Pricing Procedure."
    ),
    "condition record": (
        "Condition Records in SAP SD store the actual values for pricing conditions. "
        "Created via VK11, maintained in VK12, displayed in VK13. A condition record is valid "
        "for a specific key combination — e.g., customer + material, or price list + material. "
        "Each condition record has a validity period (valid from/to dates) and a scale "
        "for quantity or value-based pricing. The pricing engine reads condition records "
        "during order entry based on the access sequence."
    ),
    "inquiry and quotation": (
        "An Inquiry (transaction VA11) is a customer request for information about prices and "
        "delivery without commitment. It is a pre-sales document with no legal obligation. "
        "A Quotation (VA21) is the company's formal offer to a customer with confirmed prices, "
        "quantities and delivery dates — it is legally binding for the validity period. "
        "Both are reference documents that can be copied into Sales Orders via copy control. "
        "Stored in tables VBAK/VBAP like other SD documents."
    ),
    "sd integrate with mm and fi": (
        "SAP SD integrates with MM: availability check (ATP) reads MM stock levels, goods issue "
        "in SD triggers MM inventory movement (movement type 601), material master data is shared. "
        "SD integrates with FI: the billing document automatically creates an FI accounting "
        "document posting to customer receivables (debit) and revenue accounts (credit). "
        "Account determination uses VKOA configuration to map SD condition types to G/L accounts. "
        "The goods issue creates a stock account posting in both MM and FI simultaneously."
    ),
    "route determination": (
        "Route Determination in SAP SD defines the shipping path from the delivering plant to "
        "the ship-to party. The route is determined automatically based on: shipping point, "
        "transportation zone of the ship-to party, shipping condition from the customer master, "
        "and weight group. Routes are configured in OVTC. The route determines the transit time "
        "which affects the planned goods issue date calculation in the delivery."
    ),
    "inbound and outbound delivery": (
        "Outbound Delivery (LIKP/LIPS) is created from a Sales Order for goods going out to "
        "customers. Processed via VL01N. Inbound Delivery is created from a Purchase Order for "
        "goods arriving from vendors. Processed via VL31N. Both trigger warehouse management "
        "activities (picking/putaway) and inventory movements (goods issue/goods receipt). "
        "Outbound delivery posts movement type 601 (GI). Inbound delivery posts movement type 101 (GR)."
    ),

    # ── SAP MM — Materials Management ─────────────────────────────────────────
    "what is a purchase order": (
        "A Purchase Order (PO) in SAP MM is a formal procurement document sent to a vendor. "
        "Created via ME21N. Contains header data (vendor, payment terms, currency, purchasing org) "
        "and line items (material, quantity, delivery date, price, account assignment). "
        "Stored in EKKO (header) and EKPO (items). The PO is the reference document for "
        "Goods Receipt (MIGO) and Invoice Verification (MIRO). PO types: NB (standard), "
        "FO (framework order), KK (consignment)."
    ),
    "procurement process": (
        "The Procurement Process in SAP MM: 1) Purchase Requisition (ME51N) — internal request "
        "to purchase. 2) Request for Quotation (ME41) — invite vendor bids. "
        "3) Quotation Comparison (ME49) — evaluate vendor offers. "
        "4) Purchase Order (ME21N) — formal order to vendor. "
        "5) Goods Receipt (MIGO) — record material arrival, movement type 101. "
        "6) Invoice Verification (MIRO) — 3-way match (PO + GR + Invoice). "
        "7) Payment — processed in FI via F110 (automatic payment run)."
    ),
    "purchase requisition": (
        "A Purchase Requisition (PR) in SAP MM is an internal document requesting procurement "
        "of materials or services. Created manually via ME51N or automatically by MRP (MD01). "
        "Contains plant, material, quantity, required delivery date, and account assignment. "
        "Stored in EBAN. PRs are converted to POs using ME57 (assign and process) or "
        "ME59N (automatic PO creation). Approval workflows can be configured per PR value."
    ),
    "purchase info record": (
        "A Purchase Info Record (PIR) in SAP MM stores the purchasing relationship between a "
        "material and a vendor. Contains the vendor's price, currency, delivery time, and "
        "order unit. Created via ME11, displayed via ME13. Used as the default source of "
        "pricing when creating Purchase Orders. Info record categories: Standard, Consignment, "
        "Subcontracting, Pipeline. Stored in EINA (general data) and EINE (purchasing org data)."
    ),
    "goods receipt": (
        "Goods Receipt (GR) in SAP MM is posted using transaction MIGO with movement type 101 "
        "(GR against Purchase Order). It records physical arrival of materials, increases "
        "warehouse stock, and creates an accounting document. The GR reduces the open PO "
        "quantity and creates a GR/IR clearing account posting. Stored in MKPF (header) and "
        "MSEG (items). After GR, the vendor invoice can be verified in MIRO."
    ),
    "invoice verification": (
        "Invoice Verification in SAP MM (transaction MIRO) performs the 3-way match: "
        "Purchase Order + Goods Receipt + Vendor Invoice. The system checks quantity and price "
        "tolerances. On posting: vendor liability account credited, GR/IR clearing account debited. "
        "Blocked invoices (tolerance exceeded) can be released via MRBR. "
        "Evaluated Receipt Settlement (ERS) automates invoice creation from GR data "
        "without a paper invoice."
    ),
    "material master": (
        "The Material Master in SAP is the central repository for all material data. "
        "Views include: Basic Data (general info), Purchasing (purchasing group, lead time), "
        "MRP (MRP type, lot size, safety stock), Storage (storage conditions), "
        "Sales (delivering plant, item category group), Accounting (valuation class, price). "
        "Stored in MARA (general), MARC (plant), MARD (storage location). "
        "Created via MM01, changed via MM02, displayed via MM03."
    ),
    "vendor master": (
        "The Vendor Master in SAP stores all information about suppliers. "
        "Contains three data levels: General Data (name, address, bank details — table LFA1), "
        "Company Code Data (payment terms, reconciliation account — table LFB1), "
        "Purchasing Org Data (incoterms, order currency, minimum order value — table LFM1). "
        "Created via MK01 (purchasing) or FK01 (accounting). "
        "Vendor master changes are logged and can require dual control."
    ),
    "purchasing organization": (
        "A Purchasing Organisation in SAP MM is the organisational unit responsible for "
        "procurement activities. It negotiates purchasing conditions with vendors and is "
        "assigned to one or more plants. Types: plant-specific (one plant), cross-plant "
        "(multiple plants), and cross-company (multiple company codes). "
        "Configured in OMEO. The purchasing org determines which info records, contracts "
        "and scheduling agreements apply."
    ),
    "movement types in sap": (
        "Movement Types in SAP MM control how inventory quantities and values are updated. "
        "Key movement types: 101 GR against PO, 102 reversal of GR, 201 GI to cost center, "
        "261 GI to production order, 301 transfer posting plant to plant, "
        "311 transfer posting storage location to storage location, "
        "501 GR without PO (no account assignment), 601 GI for outbound delivery (SD). "
        "Each movement type has a configured account determination in OBYC."
    ),
    "inventory management": (
        "Inventory Management in SAP MM handles all stock movements and valuations. "
        "Key transactions: MIGO (goods movements), MB52 (warehouse stocks), "
        "MB51 (material document list), MI01/MI04 (physical inventory). "
        "Stock types: unrestricted use, quality inspection, blocked, consignment. "
        "Valuation methods: standard price (S) or moving average price (V) per material. "
        "Physical inventory process: create inventory document (MI01), enter count (MI04), "
        "post differences (MI07)."
    ),
    "consignment purchase order": (
        "A Consignment Purchase Order in SAP MM (PO type KK) allows vendor goods to be stored "
        "at the company premises without immediate ownership transfer. Payment is made only "
        "when goods are withdrawn from consignment stock. The GR posts to consignment stock "
        "(movement type 101 K). Withdrawal from consignment (movement type 201 K or 261 K) "
        "triggers invoice creation via MRKO (consignment and pipeline settlement)."
    ),

    # ── SAP BTP & Cloud Integration ───────────────────────────────────────────
    "what is sap btp": (
        "SAP Business Technology Platform (BTP) is SAP's unified platform for application "
        "development, integration, data management and analytics. Four pillars: "
        "1) Application Development — Cloud Foundry, ABAP Environment, Kyma (Kubernetes). "
        "2) Integration — SAP Integration Suite (CPI, API Management, Event Mesh). "
        "3) Data and Analytics — SAP HANA Cloud, SAP Analytics Cloud, SAP Datasphere. "
        "4) AI and Automation — SAP AI Core, SAP Build Process Automation. "
        "BTP is the foundation for extending and connecting SAP S/4HANA."
    ),
    "sap cloud platform integration": (
        "SAP Cloud Platform Integration (CPI), now SAP Integration Suite — Cloud Integration, "
        "is a middleware platform for connecting cloud and on-premise applications. "
        "Supports adapters: SOAP, REST/HTTP, OData, SFTP, JDBC, IDoc, AS2, Mail and 50+ others. "
        "Integration Flows (iFlows) define the message pipeline with steps: routing, mapping, "
        "content modifier, Groovy scripts, and adapters. Deployed on SAP BTP and monitored "
        "via the Operations view. Pricing is based on message packages."
    ),
    "btp connectivity service": (
        "SAP BTP Connectivity Service enables secure connections between BTP cloud applications "
        "and on-premise systems. Works through the SAP Cloud Connector — a reverse proxy "
        "installed in the customer network that opens an outbound TLS tunnel to BTP. "
        "No inbound firewall ports are required. The Cloud Connector (transaction SCC_MAIN) "
        "whitelists allowed backend systems, resources and protocols. "
        "Credentials are stored in BTP Destinations (transaction BTP Cockpit → Destinations)."
    ),
    "how does sap cpi work": (
        "SAP CPI works by executing Integration Flows (iFlows) on the BTP runtime. "
        "A sender system sends a message to CPI via a sender adapter (HTTPS, SFTP, IDoc etc.). "
        "The iFlow processes the message: transforms it (mapping, Groovy script, content modifier), "
        "routes it based on conditions, and forwards it to the receiver via a receiver adapter. "
        "CPI handles message persistence, error handling, retry logic and monitoring. "
        "iFlows are built in the Integration Suite web designer and deployed to the runtime."
    ),
    "integration flow in sap cpi": (
        "An Integration Flow (iFlow) in SAP CPI is the graphical definition of a message "
        "processing pipeline. Key components: Sender (source system + adapter), "
        "Start Message Event, Processing steps (Content Modifier, Message Mapping, Groovy Script, "
        "Router, Splitter, Aggregator, Filter), End Message Event, Receiver (target system + adapter). "
        "iFlows are created in the Integration Suite designer, packaged in Integration Packages, "
        "and deployed to the CPI runtime. Monitored in the Operations view."
    ),
    "btp neo environment": (
        "SAP BTP Neo Environment is the original BTP environment based on SAP's own Java runtime. "
        "It supports Java, HTML5, and SAP HANA development. Neo is the older environment; "
        "SAP now recommends Cloud Foundry or Kyma for new projects. "
        "Neo provides services like Connectivity, Document Management, and Portal. "
        "Key difference from Cloud Foundry: Neo uses SAP-proprietary APIs while "
        "Cloud Foundry follows open-source standards."
    ),
    "sap cloud platform abap environment": (
        "SAP BTP ABAP Environment (also called Steampunk) provides a managed ABAP runtime "
        "on SAP BTP for building cloud-native ABAP applications and extensions. "
        "It supports the ABAP RESTful Application Programming Model (RAP), CDS views, "
        "OData V4 services, and ABAP Unit testing. Accessed via ADT (Eclipse). "
        "No direct database access — all data access through CDS views and ABAP managed objects. "
        "Used for cloud extensions to SAP S/4HANA Cloud."
    ),
    "sap btp connect to s4hana": (
        "SAP BTP connects to SAP S/4HANA using: 1) SAP Cloud Connector — for on-premise S/4HANA, "
        "creates a secure tunnel via the Connectivity Service. 2) BTP Destinations — store "
        "connection parameters (URL, authentication, proxy type). 3) SAP Integration Suite (CPI) "
        "— for message-based integration using iFlows and adapters. "
        "4) SAP API Business Hub — pre-built OData APIs for S/4HANA Cloud. "
        "5) Event-driven integration — using SAP Event Mesh for asynchronous messaging."
    ),

    # ── SAP S/4HANA ───────────────────────────────────────────────────────────
    "what is sap s4hana": (
        "SAP S/4HANA is SAP's next-generation intelligent ERP suite built on the SAP HANA "
        "in-memory database. Key differences from SAP ECC: "
        "Simplified data model — ACDOCA (Universal Journal) replaces multiple FI/CO tables. "
        "Real-time analytics — no need for aggregates or batch reporting. "
        "Modern UX — SAP Fiori replaces SAP GUI for most transactions. "
        "Smaller database footprint — data compression and elimination of redundant tables. "
        "Available as S/4HANA Cloud (public/private) and S/4HANA on-premise."
    ),
    "third party logistics with s4hana": (
        "Third-Party Logistics (3PL) integration with SAP S/4HANA uses SAP Integration Suite "
        "to connect S/4HANA with external logistics providers. Key scenarios: "
        "Outbound Delivery to 3PL (ship notice), Goods Issue confirmation from 3PL, "
        "Warehouse status updates, and Track-and-trace events. "
        "Integration uses standard OData APIs (A_OutbDeliveryHeader, A_GoodsMovement) "
        "or IDoc messages (DESADV, SHPORD). SAP LBN (Logistics Business Network) provides "
        "a managed platform for 3PL collaboration."
    ),
    "s4hana migration process": (
        "The SAP S/4HANA Migration Process follows SAP Activate methodology: "
        "1) Prepare — system sizing, landscape planning, project setup. "
        "2) Explore — fit-gap analysis, custom code adaptation (using SAP Readiness Check). "
        "3) Realize — system conversion or new implementation, data migration (LTMC/LTMOM). "
        "4) Deploy — go-live preparation, cutover planning. "
        "5) Run — post go-live support and optimisation. "
        "Migration paths: System Conversion (brownfield), New Implementation (greenfield), "
        "or Selective Data Transition (bluefield)."
    ),
    "ltmc in s4hana": (
        "LTMC (Legacy Transfer Migration Cockpit) in SAP S/4HANA is the tool for migrating "
        "legacy data during an S/4HANA implementation. It supports template-based migration "
        "using Excel or XML files, and direct transfer from SAP source systems. "
        "Supports migration objects for master data (customers, vendors, materials) and "
        "open items (open POs, open SOs, open FI items). Accessed via transaction LTMC. "
        "Replaces the older LSMW (Legacy System Migration Workbench) for most scenarios."
    ),

    # ── ABAP Development ──────────────────────────────────────────────────────
    "what is an abap badi": (
        "A BAdI (Business Add-In) in ABAP is an enhancement technology that allows custom code "
        "to be called at predefined extension points in SAP standard programs without modifying "
        "standard code. BAdIs use ABAP interfaces — you implement the interface methods with "
        "your custom logic. Classic BAdIs: transaction SE18 (definition), SE19 (implementation). "
        "New/Kernel BAdIs use Enhancement Spots and the BADI_DEFINITION workbench object. "
        "Multiple implementations can be active simultaneously; filtered BAdIs activate based on "
        "context values."
    ),
    "user exit in abap": (
        "User Exits in SAP ABAP are predefined FORM subroutines in standard programs that can "
        "be filled with custom code. They use function module exits with naming convention "
        "EXIT_<programname>_<number> (e.g., EXIT_SAPMV45A_001 for SD Sales Order). "
        "Found using transaction SMOD, implemented via CMOD (create enhancement project). "
        "Unlike BAdIs, only one active implementation is allowed per exit. "
        "Modern SAP recommends BAdIs and Enhancement Spots instead of classic user exits."
    ),
    "abap restful programming model": (
        "The ABAP RESTful Application Programming Model (RAP) is the modern development "
        "framework for SAP Fiori apps and OData services on BTP ABAP Environment and S/4HANA. "
        "RAP uses: CDS Views for data modeling, Behaviour Definitions (BDEF) for business logic "
        "(create/update/delete/actions), Service Definitions and Service Bindings for OData V2/V4 "
        "exposure. Supports managed and unmanaged scenarios. Replaces BOPF (Business Object "
        "Processing Framework). Built with ADT (Eclipse) using ABAP Development Tools."
    ),
    "abap class": (
        "An ABAP Class is the fundamental building block of ABAP Object-Oriented Programming. "
        "Defined with CLASS <name> DEFINITION ... ENDCLASS and implemented with "
        "CLASS <name> IMPLEMENTATION ... ENDCLASS. Classes have: Attributes (data), "
        "Methods (behaviour), Events, and Interfaces. Visibility sections: PUBLIC (accessible "
        "from anywhere), PROTECTED (subclasses only), PRIVATE (class only). "
        "Created and maintained via transaction SE24 (Class Builder) or ADT in Eclipse."
    ),
    "abap object oriented": (
        "ABAP Object-Oriented Programming uses Classes and Interfaces. Key OOP concepts in ABAP: "
        "Encapsulation — visibility sections (PUBLIC, PROTECTED, PRIVATE). "
        "Inheritance — INHERITING FROM superclass, method redefinition. "
        "Polymorphism — interface references, dynamic method dispatch. "
        "Instantiation: CREATE OBJECT or NEW operator. Static vs instance members: "
        "CLASS-DATA and CLASS-METHODS are static. Interfaces (INTERFACE...ENDINTERFACE) "
        "define contracts implemented by multiple classes."
    ),
    "abap function module": (
        "An ABAP Function Module is a reusable code block with a defined interface containing "
        "IMPORTING, EXPORTING, CHANGING, and TABLES parameters plus EXCEPTIONS. "
        "Grouped in Function Groups (SE37 to create/display). Called with CALL FUNCTION. "
        "Remote-enabled Function Modules (RFC) can be called from external systems or "
        "other SAP systems. Common RFCs: RFC_READ_TABLE (read any DB table), "
        "BAPI_SALESORDER_CREATEFROMDAT2 (create Sales Order), RFC_GET_SYSTEM_INFO. "
        "Modern ABAP prefers methods in classes over function modules."
    ),
    "for all entries in abap": (
        "FOR ALL ENTRIES (FAE) in ABAP SELECT reads database records matching values in an "
        "internal table. Syntax: SELECT fields FROM table INTO TABLE itab "
        "FOR ALL ENTRIES IN itab_source WHERE key = itab_source-field. "
        "Critical rules: 1) Check the source table is NOT EMPTY before FAE — if empty, "
        "all records are selected. 2) Include the join key in SELECT fields. "
        "3) Duplicates in result are removed automatically. "
        "4) Performance: keep source table small; use JOIN or subquery for large datasets."
    ),
    "badi vs enhancement spot": (
        "BAdI (Business Add-In): object-oriented enhancement point defined with a specific "
        "interface. Implemented via SE19 or ABAP in SE80/ADT. Can have multiple active "
        "implementations; supports filter-based activation. "
        "Enhancement Spot: the container that holds BAdI definitions and explicit enhancement "
        "sections/points. An Enhancement Spot can contain multiple BAdIs. "
        "Explicit enhancements (ENHANCEMENT...END ENHANCEMENT) allow inserting code at "
        "specific lines. Implicit enhancements (at the start/end of programs or function modules) "
        "use enhancement points without source changes."
    ),

    # ── General SAP ───────────────────────────────────────────────────────────
    "what is general ledger": (
        "The General Ledger (G/L) in SAP FI is the central record of all financial transactions. "
        "In S/4HANA it uses the Universal Journal (table ACDOCA) which combines G/L, Controlling, "
        "Asset Accounting, and Profit Center data in a single line-item table. "
        "G/L accounts are defined in the Chart of Accounts (transaction FS00). "
        "Postings via FB01, displayed in FB03. The Trial Balance and Financial Statements "
        "are generated from G/L data using transaction F.01 or S_ALR_87012284."
    ),
    "odata service in sap": (
        "OData (Open Data Protocol) is a REST-based protocol used by SAP to expose data as APIs. "
        "SAP S/4HANA exposes standard OData services via the API Business Hub (api.sap.com). "
        "Key services: API_SALES_ORDER_SRV (Sales Orders), API_PURCHASEORDER_PROCESS_SRV "
        "(Purchase Orders), API_BUSINESS_PARTNER (Business Partners). "
        "OData V2 uses $metadata, $filter, $expand. OData V4 adds computed properties and actions. "
        "In SAP CPI, OData services are consumed via the HTTP adapter with OData-specific config."
    ),
    "what is journal entry": (
        "A Journal Entry in SAP FI is a manual financial posting that records a transaction "
        "in the General Ledger. Created via transaction FB01 or F-02. A journal entry must "
        "balance (debits = credits). It consists of a document header (company code, posting date, "
        "document type, reference) and line items (G/L account, amount, cost center). "
        "In S/4HANA, journal entries are stored in ACDOCA. Standard document types: "
        "SA (G/L account document), AA (asset posting), KR (vendor invoice), DR (customer invoice)."
    ),
    "what is company code": (
        "A Company Code in SAP is the smallest organisational unit for which a complete set of "
        "accounts can be drawn up. It represents a legal entity for financial accounting purposes. "
        "Configured in transaction OX02. Assigned to a controlling area, credit control area, "
        "and plants. Each company code has its own chart of accounts, fiscal year variant, "
        "and currency. Financial statements (balance sheet, P&L) are produced per company code. "
        "Stored in table T001."
    ),
    "what is cost center": (
        "A Cost Center in SAP CO (Controlling) is an organisational unit within a controlling "
        "area that represents a location of cost incurrence. Used for internal cost accounting "
        "and reporting. Created via KS01, displayed via KS03. Cost Centers are assigned to "
        "a cost center hierarchy (controlling area → cost center group → cost center). "
        "Costs are posted to cost centers from FI documents (account assignments). "
        "Stored in table CSKS. Grouped for reporting via cost center groups (KSH1)."
    ),
    "what is project": (
        "A Project in SAP PS (Project System) is used to plan, execute, and monitor complex "
        "tasks. The Project Definition (PROJ table) is the top-level container. "
        "Work Breakdown Structure (WBS) elements (PRPS table) represent the hierarchical "
        "breakdown of project scope. Networks and Activities define the work sequence and "
        "scheduling. Created via CJ01, displayed via CJ03. Projects integrate with CO for "
        "cost planning (CJ30), MM for material procurement, and FI for actual cost settlement."
    ),
    "what is business partner": (
        "A Business Partner (BP) in SAP is the central master data object representing any "
        "person or organisation that a company has a business relationship with. "
        "In S/4HANA, BP replaces separate customer (KNA1) and vendor (LFA1) master records "
        "as the single point of entry. Transaction BP. Roles define the business context: "
        "FLCU01 = Customer, FLVN01 = Vendor, FLCU00 = FI Customer. "
        "BP data is shared across modules (SD, MM, FI) ensuring data consistency."
    ),
    "what is work order": (
        "A Work Order in SAP PM (Plant Maintenance) or PP (Production) is a document used to "
        "plan and execute maintenance or production tasks. PM Work Orders (IW31) plan "
        "maintenance activities, assign personnel and materials, and record actual costs. "
        "PP Production Orders (CO01) plan the manufacture of a product, defining operations, "
        "components, and routing. Work orders integrate with MM for material reservations, "
        "CO for cost postings, and HR for time confirmations."
    ),
    "what is supplier invoice": (
        "A Supplier Invoice in SAP MM/FI is the vendor's bill for goods or services delivered. "
        "Processed via MIRO (logistics invoice verification) for PO-based invoices, or "
        "via FB60 (direct FI posting) for non-PO invoices. MIRO performs 3-way matching "
        "(PO + GR + Invoice). The posted invoice creates: vendor liability (credit), "
        "GR/IR clearing account (debit for PO-based), and tax line items. "
        "In S/4HANA: API_SUPPLIERINVOICE_PROCESS_SRV is the OData API for supplier invoices."
    ),
}

# ── Alias map: maps query variations to KB keys ───────────────────────────────
_KB_ALIASES = {
    "what sales order": "what is a sales order",
    "what is sales order": "what is a sales order",
    "sales order process in sap": "sales order process",
    "sap sd process": "sales order process",
    "billing document in sap": "billing document",
    "what is billing": "billing document",
    "outbound delivery": "delivery from sales order",
    "delivery in sap": "delivery from sales order",
    "what is delivery": "delivery from sales order",
    "partner function in sap": "partner function",
    "partner functions": "partner function",
    "pricing work in sap": "pricing in sap sd",
    "how does pricing": "pricing in sap sd",
    "what is pricing": "pricing in sap sd",
    "condition technique": "pricing in sap sd",
    "condition records": "condition record",
    "what is a condition": "condition record",
    "inquiry quotation": "inquiry and quotation",
    "difference between inquiry": "inquiry and quotation",
    "how is delivery": "delivery from sales order",
    "how is a delivery": "delivery from sales order",
    "quotation in sap": "inquiry and quotation",
    "sap sd integrate": "sd integrate with mm and fi",
    "sd mm fi": "sd integrate with mm and fi",
    "how does sap sd": "sd integrate with mm and fi",
    "purchase order in sap": "what is a purchase order",
    "what is purchase order": "what is a purchase order",
    "what is po": "what is a purchase order",
    "goods receipt in sap": "goods receipt",
    "how does goods receipt": "goods receipt",
    "what is gr": "goods receipt",
    "invoice verification in sap": "invoice verification",
    "what is miro": "invoice verification",
    "material master in sap": "material master",
    "what is material master": "material master",
    "vendor master in sap": "vendor master",
    "how is vendor": "vendor master",
    "purchasing organization": "purchasing organization",
    "purchasing organisation": "purchasing organization",
    "movement types": "movement types in sap",
    "what are movement": "movement types in sap",
    "what is sap btp": "what is sap btp",
    "what is btp": "what is sap btp",
    "btp platform": "what is sap btp",
    "sap cloud platform integration": "sap cloud platform integration",
    "what is cpi": "sap cloud platform integration",
    "what is sap cpi": "sap cloud platform integration",
    "connectivity service": "btp connectivity service",
    "cloud connector": "btp connectivity service",
    "sap cpi work": "how does sap cpi work",
    "how does cpi": "how does sap cpi work",
    "integration flow": "integration flow in sap cpi",
    "what is iflow": "integration flow in sap cpi",
    "what is an iflow": "integration flow in sap cpi",
    "neo environment": "btp neo environment",
    "btp neo": "btp neo environment",
    "abap environment": "sap cloud platform abap environment",
    "steampunk": "sap cloud platform abap environment",
    "btp abap": "sap cloud platform abap environment",
    "btp connect": "sap btp connect to s4hana",
    "migration process": "s4hana migration process",
    "s4hana migration": "s4hana migration process",
    "migrate to s4hana": "s4hana migration process",
    "sap s4hana migration": "s4hana migration process",
    "sap s4hana": "what is sap s4hana",
    "what is s4hana": "what is sap s4hana",
    "third party logistics": "third party logistics with s4hana",
    "3pl": "third party logistics with s4hana",
    "migration process": "s4hana migration process",
    "s4hana migration": "s4hana migration process",
    "migrate to s4hana": "s4hana migration process",
    "ltmc": "ltmc in s4hana",
    "what is ltmc": "ltmc in s4hana",
    "badi": "what is an abap badi",
    "what is badi": "what is an abap badi",
    "what is a badi": "what is an abap badi",
    "user exit": "user exit in abap",
    "what is user exit": "user exit in abap",
    "abap restful": "abap restful programming model",
    "rap model": "abap restful programming model",
    "abap rap": "abap restful programming model",
    "abap class": "abap class",
    "what is abap class": "abap class",
    "abap oop": "abap object oriented",
    "object oriented abap": "abap object oriented",
    "function module": "abap function module",
    "what is function module": "abap function module",
    "for all entries": "for all entries in abap",
    "badi enhancement spot": "badi vs enhancement spot",
    "general ledger": "what is general ledger",
    "what is gl": "what is general ledger",
    "odata": "odata service in sap",
    "what is odata": "odata service in sap",
    "journal entry": "what is journal entry",
    "what is journal": "what is journal entry",
    "company code": "what is company code",
    "what is company code": "what is company code",
    "cost center": "what is cost center",
    "what is cost center": "what is cost center",
    "project in sap": "what is project",
    "what is project": "what is project",
    "work breakdown": "what is project",
    "wbs": "what is project",
    "business partner": "what is business partner",
    "what is business partner": "what is business partner",
    "work order": "what is work order",
    "what is work order": "what is work order",
    "supplier invoice": "what is supplier invoice",
    "what is supplier invoice": "what is supplier invoice",
    "route determination": "route determination",
    "inbound delivery": "inbound and outbound delivery",
    "outbound delivery sap": "inbound and outbound delivery",
    "inventory management": "inventory management",
    "what is inventory": "inventory management",
    "purchase info record": "purchase info record",
    "info record": "purchase info record",
    "consignment": "consignment purchase order",
    "procurement process": "procurement process",
    "purchase requisition": "purchase requisition",
    "what is pr": "purchase requisition",
    "item categories": "item categories in sales orders",
    "item category": "item categories in sales orders",
    "copy control": "copy control",
}


def _kb_answer(query: str) -> str:
    """
    Look up the built-in SAP Knowledge Base.
    Uses exact alias matching first, then phrase-in-query matching.
    Returns clean answer string or empty string if not found.
    """
    q = query.lower().strip().rstrip('?').strip()

    # 1. Check alias map first (most precise)
    for alias, kb_key in _KB_ALIASES.items():
        if alias in q:
            ans = SAP_KB.get(kb_key, '')
            if ans:
                return ans

    # 2. Check direct KB key match
    for key, answer in SAP_KB.items():
        if key in q:
            return answer

    # 3. Word-level match — all words of key must appear in query
    q_words = set(re.findall(r'[a-z]{3,}', q))
    best_score = 0
    best_answer = ''
    for key, answer in SAP_KB.items():
        key_words = set(re.findall(r'[a-z]{3,}', key))
        if not key_words:
            continue
        # Score = fraction of key words found in query
        matches = len(q_words & key_words)
        # Require at least half the key words to match
        if matches >= max(1, len(key_words) // 2):
            score = matches / len(key_words)
            if score > best_score:
                best_score = score
                best_answer = answer

    return best_answer if best_score >= 0.5 else ''


def _web_answer_safe(query: str) -> str:
    """Try web search — returns empty string on any failure."""
    try:
        result = _web_answer(query)
        return result if result else ''
    except Exception:
        return ''


def answer_from_docs(query: str) -> str:
    """
    Answer engine — three layers, all independent:
    1. Web search (if internet available)
    2. Built-in SAP Knowledge Base (always works — no internet needed)
    3. Not found — helpful message
    """
    topic = query.strip().rstrip('?').title()

    # ── Layer 1: Web search ────────────────────────────────────────────────────
    try:
        web = _web_answer_safe(query)
        if web and len(web.split()) >= 15:
            return (
                '🌐 **' + topic + '**\n\n' +
                web + '\n\n' +
                '---\n' +
                '*Source: Web Search (DuckDuckGo + SAP Help)*'
            )
    except Exception:
        pass

    # ── Layer 2: Built-in SAP Knowledge Base ──────────────────────────────────
    kb = _kb_answer(query)
    if kb:
        return (
            '📖 **' + topic + '**\n\n' +
            kb + '\n\n' +
            '---\n' +
            '*Source: SAP Knowledge Base (built-in)*'
        )

    # ── Layer 3: Not found ────────────────────────────────────────────────────
    return (
        '📖 **' + topic + '**\n\n' +
        'I don\'t have a specific answer for this topic yet.\n\n' +
        'Try one of these formats:\n' +
        '- *"What is a Purchase Order in SAP?"*\n' +
        '- *"How does Goods Receipt work in SAP?"*\n' +
        '- *"What is the SAP S/4HANA migration process?"*\n' +
        '- *"What is a BAdI in ABAP?"*'
    )


def _web_answer_abap(prompt: str) -> str:
    """
    ABAP handler:
    - Code request for known entity → local ABAP generator (always works)
    - Concept/explanation question → KB answer + web
    """
    entity_key = _detect_abap_entity(prompt)
    t = prompt.lower()

    concept_kws = ['what is', 'explain', 'how does', 'tell me', 'define',
                   'what are', 'describe', 'difference between', 'how do']
    is_concept = any(kw in t for kw in concept_kws)

    # Code generation request — use enhanced dispatcher (CDS/RAP/Class/Program)
    if entity_key and not is_concept:
        return generate_abap_enhanced(prompt)
    # Also handle advanced requests (CDS/RAP/commitment) even without entity match
    if is_advanced_abap_request(prompt) and not is_concept:
        return generate_abap_enhanced(prompt)

    # Concept question — try KB first
    kb = _kb_answer(prompt)
    if kb:
        topic = prompt.strip().rstrip('?').title()
        return (
            '📖 **' + topic + '**\n\n' + kb + '\n\n' +
            '---\n*Source: SAP Knowledge Base (built-in)*'
        )

    # Try web
    web = _web_answer_safe(prompt)
    if web and len(web.split()) >= 15:
        topic = prompt.strip().rstrip('?').title()
        return (
            '🌐 **' + topic + '**\n\n' + web + '\n\n' +
            '---\n*Source: Web Search*'
        )

    # Final fallback — local ABAP code if entity found
    if entity_key or is_advanced_abap_request(prompt):
        return generate_abap_enhanced(prompt)

    return (
        '📖 **ABAP Help**\n\n' +
        'Please specify what you need:\n' +
        '- For ABAP code: *"Give me ABAP for Sales Orders"*\n' +
        '- For concepts: *"What is a BAdI?"* or *"Explain FOR ALL ENTRIES"*'
    )


def is_functional_question(text: str) -> bool:
    """
    Returns True if the prompt is a functional/knowledge question
    rather than an iFlow generation request.
    Checks for question patterns AND absence of iFlow generation keywords.
    """
    t = text.lower().strip()

    # iFlow generation keywords — if present, it's a generation request
    iflow_kws = [
        "iflow", "i-flow", "generate", "create iflow", "build iflow",
        "post iflow", "get iflow", "put iflow", "delete iflow",
        "groovy", "odata", "http adapter", "sender adapter",
        "smartapp one package", "new package",
    ]
    if any(kw in t for kw in iflow_kws):
        return False

    # Functional question patterns
    func_patterns = [
        r"^what is\b", r"^what are\b", r"^what does\b",
        r"^explain\b",  r"^describe\b",  r"^tell me about\b",
        r"^how does\b", r"^how do\b",    r"^how is\b",
        r"^define\b",   r"^definition of\b",
        r"^what fields\b", r"^list.*fields\b",
        r"^what.*process\b", r"^show me.*process\b",
        r"^give me.*overview\b", r"^overview of\b",
        r"^difference between\b", r"^compare\b",
        r"^when (is|do|does|should)\b",
        r"^why (is|are|do|does)\b",
        r"^which\b",
        r"^can you explain\b", r"^could you explain\b",
        r"^i want to know\b", r"^i need to know\b",
        r"^help me understand\b",
    ]
    if any(re.search(p, t) for p in func_patterns):
        return True

    # Ends with question mark and no iFlow keyword
    if t.endswith("?"):
        return True

    return False


# (answer_from_docs defined above)

# ═══════════════════════════════════════════════════════════════════════════════
# ABAP CODE GENERATOR
# Pattern library for common SAP entities — generates clean, runnable ABAP
# ═══════════════════════════════════════════════════════════════════════════════

_ABAP_ENTITIES = {
    "sales order": {
        "label": "Sales Orders and Items",
        "header_tbl": "VBAK", "item_tbl": "VBAP",
        "header_key": "VBELN", "join_key": "VBELN",
        "header_fields": ["VBELN","ERDAT","AUART","KUNNR","NETWR","WAERK"],
        "item_fields":   ["VBELN","POSNR","MATNR","ARKTX","KWMENG","VRKME","NETPR"],
        "header_desc": "Sales Order Header", "item_desc": "Sales Order Items",
        "where_clause": "ERDAT GE @lv_date_from", "date_field": "ERDAT",
    },
    "purchase order": {
        "label": "Purchase Orders and Items",
        "header_tbl": "EKKO", "item_tbl": "EKPO",
        "header_key": "EBELN", "join_key": "EBELN",
        "header_fields": ["EBELN","AEDAT","BSART","LIFNR","BUKRS","WAERS"],
        "item_fields":   ["EBELN","EBELP","MATNR","TXZ01","MENGE","MEINS","NETPR"],
        "header_desc": "Purchase Order Header", "item_desc": "Purchase Order Items",
        "where_clause": "AEDAT GE @lv_date_from", "date_field": "AEDAT",
    },
    "delivery": {
        "label": "Outbound Deliveries and Items",
        "header_tbl": "LIKP", "item_tbl": "LIPS",
        "header_key": "VBELN", "join_key": "VBELN",
        "header_fields": ["VBELN","ERDAT","LFART","KUNNR","WAERK"],
        "item_fields":   ["VBELN","POSNR","MATNR","ARKTX","LFIMG","VRKME"],
        "header_desc": "Delivery Header", "item_desc": "Delivery Items",
        "where_clause": "ERDAT GE @lv_date_from", "date_field": "ERDAT",
    },
    "billing": {
        "label": "Billing Documents and Items",
        "header_tbl": "VBRK", "item_tbl": "VBRP",
        "header_key": "VBELN", "join_key": "VBELN",
        "header_fields": ["VBELN","FKDAT","FKART","KUNRG","NETWR","WAERK"],
        "item_fields":   ["VBELN","POSNR","MATNR","ARKTX","FKIMG","VRKME","NETWR"],
        "header_desc": "Billing Document Header", "item_desc": "Billing Document Items",
        "where_clause": "FKDAT GE @lv_date_from", "date_field": "FKDAT",
    },
    "purchase requisition": {
        "label": "Purchase Requisitions",
        "header_tbl": "EBAN", "item_tbl": None,
        "header_key": "BANFN", "join_key": None,
        "header_fields": ["BANFN","BADAT","BSART","AFNAM","MATNR","TXZ01","MENGE","MEINS"],
        "item_fields":   [],
        "header_desc": "Purchase Requisition", "item_desc": None,
        "where_clause": "BADAT GE @lv_date_from", "date_field": "BADAT",
    },
    "material": {
        "label": "Material Master",
        "header_tbl": "MARA", "item_tbl": "MARD",
        "header_key": "MATNR", "join_key": "MATNR",
        "header_fields": ["MATNR","ERSDA","MTART","MATKL","MEINS","MBRSH"],
        "item_fields":   ["MATNR","WERKS","LGORT","LABST","EINME"],
        "header_desc": "Material Master General", "item_desc": "Material Stock",
        "where_clause": "ERSDA GE @lv_date_from", "date_field": "ERSDA",
    },
    "customer": {
        "label": "Customer Master",
        "header_tbl": "KNA1", "item_tbl": None,
        "header_key": "KUNNR", "join_key": None,
        "header_fields": ["KUNNR","ERDAT","NAME1","ORT01","LAND1","REGIO","KTOKD"],
        "item_fields":   [],
        "header_desc": "Customer Master General", "item_desc": None,
        "where_clause": "ERDAT GE @lv_date_from", "date_field": "ERDAT",
    },
    "vendor": {
        "label": "Vendor / Supplier Master",
        "header_tbl": "LFA1", "item_tbl": None,
        "header_key": "LIFNR", "join_key": None,
        "header_fields": ["LIFNR","ERDAT","NAME1","ORT01","LAND1","KTOKK"],
        "item_fields":   [],
        "header_desc": "Vendor Master General", "item_desc": None,
        "where_clause": "ERDAT GE @lv_date_from", "date_field": "ERDAT",
    },
    "general ledger": {
        "label": "General Ledger Entries",
        "header_tbl": "BKPF", "item_tbl": "BSEG",
        "header_key": "BELNR", "join_key": "BELNR",
        "header_fields": ["BUKRS","BELNR","GJAHR","BLDAT","BUDAT","BKTXT","WAERS"],
        "item_fields":   ["BUKRS","BELNR","GJAHR","BUZEI","HKONT","DMBTR","WRBTR"],
        "header_desc": "Accounting Document Header", "item_desc": "Accounting Line Items",
        "where_clause": "BUDAT GE @lv_date_from AND BUKRS EQ @lv_bukrs", "date_field": "BUDAT",
    },
    "project": {
        "label": "Project Master and WBS Elements",
        "header_tbl": "PROJ", "item_tbl": "PRPS",
        "header_key": "PSPNR", "join_key": "PSPHI",
        "header_fields": ["PSPNR","ERDAT","POST1","PROJK","VERNA","ASTNA"],
        "item_fields":   ["PSPNR","POSID","POST1","PSPHI","BELKZ","PBUKR"],
        "header_desc": "Project Definition", "item_desc": "WBS Elements",
        "where_clause": "ERDAT GE @lv_date_from", "date_field": "ERDAT",
    },
    "cost center": {
        "label": "Cost Center Master",
        "header_tbl": "CSKS", "item_tbl": None,
        "header_key": "KOSTL", "join_key": None,
        "header_fields": ["KOKRS","KOSTL","DATBI","DATAB","KTEXT","ABTEI","VERAK"],
        "item_fields":   [],
        "header_desc": "Cost Center Master Data", "item_desc": None,
        "where_clause": "DATAB LE sy-datum AND DATBI GE sy-datum", "date_field": None,
    },
    "company code": {
        "label": "Company Code Master",
        "header_tbl": "T001", "item_tbl": None,
        "header_key": "BUKRS", "join_key": None,
        "header_fields": ["BUKRS","BUTXT","ORT01","LAND1","WAERS","KTOPL"],
        "item_fields":   [],
        "header_desc": "Company Code", "item_desc": None,
        "where_clause": "", "date_field": None,
    },
}

_ABAP_ALIASES = {
    "sales orders": "sales order", "so": "sales order", "vbak": "sales order",
    "purchase orders": "purchase order", "po": "purchase order", "ekko": "purchase order",
    "deliveries": "delivery", "outbound delivery": "delivery",
    "billing document": "billing", "invoice": "billing",
    "pr": "purchase requisition",
    "materials": "material",
    "customers": "customer",
    "vendors": "vendor", "supplier": "vendor",
    "gl": "general ledger", "fi document": "general ledger", "accounting document": "general ledger",
    "projects": "project", "wbs": "project",
    "cost centres": "cost center",
}


def _detect_abap_entity(prompt: str) -> Optional[str]:
    t = prompt.lower()
    for key in _ABAP_ENTITIES:
        if key in t:
            return key
    for alias, canon in _ABAP_ALIASES.items():
        if alias in t:
            return canon
    return None


def is_abap_request(prompt: str) -> bool:
    t = prompt.lower()
    abap_kws = [
        # Original keywords
        "abap", "abap report", "abap program", "write abap",
        "give me abap", "generate abap", "abap code", "abap script",
        "abap query", "se38", "report program", "write a report",
        "write me a report", "fetch using abap", "abap to fetch",
        "abap for", "select statement",
        # v2 — CDS / RAP / OData generation
        "cds view", "define view", "define root view", "eclipse cds",
        "rap model", "rap entity", "bdef", "behaviour definition",
        "behavior definition", "odata service", "service binding",
        "service definition", "abap class", "oo abap", "class-based",
        # v2 — Financial / Commitment / Budget / Program creation
        "create a program", "create program", "generate program",
        "fetch financial", "financial commitment", "commitment data",
        "pooling from", "pool from", "budget data", "funds management",
        "fm commitment", "po and budget", "budget and po",
    ]
    return any(kw in t for kw in abap_kws)


def _build_type_block(tbl, fields, indent=4):
    pad = " " * indent
    lines = []
    for f in fields:
        lines.append(pad + f + " TYPE " + tbl + "-" + f + ",")
    # Remove trailing comma from last line
    if lines:
        lines[-1] = lines[-1].rstrip(",") + "."
    return "\n".join(lines)


def generate_abap(prompt: str) -> str:
    entity_key = _detect_abap_entity(prompt)
    if not entity_key:
        return _abap_generic()
    e = _ABAP_ENTITIES[entity_key]
    if e["item_tbl"] is None:
        return _abap_single_table(e)
    else:
        return _abap_header_items(e)


def _abap_header_items(e: dict) -> str:
    ht      = e["header_tbl"]
    it      = e["item_tbl"]
    ht_l    = ht.lower()
    it_l    = it.lower()
    hk      = e["header_key"]
    jk      = e["join_key"]
    hk_s    = hk.lower()[:6]
    hfields = ", ".join(e["header_fields"])
    ifields = ", ".join(e["item_fields"])
    where   = e["where_clause"]
    hdesc   = e["header_desc"]
    idesc   = e["item_desc"]
    label   = e["label"]
    htype   = _build_type_block(ht, e["header_fields"])
    itype   = _build_type_block(it, e["item_fields"])
    hwrite  = " ".join("ls_" + ht_l + "-" + f.lower() + "," for f in e["header_fields"][:4])
    iwrite  = " ".join("ls_" + it_l + "-" + f.lower() + "," for f in e["item_fields"][:4])

    code = (
        "*&---------------------------------------------------------------------*\n"
        "*& Report  Z_FETCH_" + ht + "\n"
        "*& Description: Fetch " + label + "\n"
        "*&---------------------------------------------------------------------*\n"
        "REPORT z_fetch_" + ht_l + ".\n\n"
        "TABLES: " + ht + ", " + it + ".\n\n"
        "SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.\n"
        "  PARAMETERS:   p_datefr TYPE d DEFAULT sy-datum.\n"
        "  SELECT-OPTIONS: s_" + hk_s + " FOR " + ht + "-" + hk + ".\n"
        "SELECTION-SCREEN END OF BLOCK b1.\n\n"
        "TYPES:\n"
        "  BEGIN OF ty_" + ht_l + ",\n"
        + htype + "\n"
        "  END OF ty_" + ht_l + ",\n\n"
        "  BEGIN OF ty_" + it_l + ",\n"
        + itype + "\n"
        "  END OF ty_" + it_l + ".\n\n"
        "DATA:\n"
        "  lt_" + ht_l + " TYPE TABLE OF ty_" + ht_l + ",\n"
        "  ls_" + ht_l + " TYPE ty_" + ht_l + ",\n"
        "  lt_" + it_l + " TYPE TABLE OF ty_" + it_l + ",\n"
        "  ls_" + it_l + " TYPE ty_" + it_l + ",\n"
        "  lv_date_from  TYPE d.\n\n"
        "INITIALIZATION.\n"
        "  lv_date_from = sy-datum - 30.\n\n"
        "START-OF-SELECTION.\n\n"
        "* --- Fetch " + hdesc + " ---\n"
        "  SELECT " + hfields + "\n"
        "    FROM " + ht + "\n"
        "    INTO TABLE @lt_" + ht_l + "\n"
        "    WHERE " + where + "\n"
        "      AND " + hk + " IN @s_" + hk_s + ".\n\n"
        "  IF sy-subrc NE 0.\n"
        "    MESSAGE 'No " + hdesc + " records found.' TYPE 'I'.\n"
        "    RETURN.\n"
        "  ENDIF.\n\n"
        "* --- Fetch " + idesc + " ---\n"
        "  SELECT " + ifields + "\n"
        "    FROM " + it + "\n"
        "    INTO TABLE @lt_" + it_l + "\n"
        "    FOR ALL ENTRIES IN @lt_" + ht_l + "\n"
        "    WHERE " + jk + " EQ @lt_" + ht_l + "-" + jk + ".\n\n"
        "END-OF-SELECTION.\n\n"
        "  WRITE: / '=== " + hdesc.upper() + " ==='.\n"
        "  LOOP AT lt_" + ht_l + " INTO ls_" + ht_l + ".\n"
        "    WRITE: / " + hwrite + ".\n"
        "    LOOP AT lt_" + it_l + " INTO ls_" + it_l + "\n"
        "         WHERE " + jk.lower() + " EQ ls_" + ht_l + "-" + jk.lower() + ".\n"
        "      WRITE: /5 " + iwrite + ".\n"
        "    ENDLOOP.\n"
        "    SKIP.\n"
        "  ENDLOOP.\n"
        "  WRITE: / 'Total " + hdesc + " Records:', lines( lt_" + ht_l + " ).\n"
    )

    return (
        "\U0001F4DD **ABAP Report \u2014 " + label + "**\n\n"
        "Tables: `" + ht + "` (" + hdesc + ")  +  `" + it + "` (" + idesc + ")\n\n"
        "```abap\n" + code + "\n```\n\n"
        "**How to use:**\n"
        "1. Open **SE38** in SAP GUI\n"
        "2. Create program `Z_FETCH_" + ht + "`\n"
        "3. Paste code, Activate (`Ctrl+F3`), Execute (`F8`)\n\n"
        "*Adjust the date range and document number selection parameters as needed.*"
    )


def _abap_single_table(e: dict) -> str:
    ht      = e["header_tbl"]
    ht_l    = ht.lower()
    hk      = e["header_key"]
    hk_s    = hk.lower()[:6]
    hfields = ", ".join(e["header_fields"])
    where   = e["where_clause"]
    hdesc   = e["header_desc"]
    label   = e["label"]
    htype   = _build_type_block(ht, e["header_fields"])
    hwrite  = " ".join("ls_result-" + f.lower() + "," for f in e["header_fields"][:5])

    sel_screen = (
        "  PARAMETERS:   p_datefr TYPE d DEFAULT sy-datum.\n"
        "  SELECT-OPTIONS: s_" + hk_s + " FOR " + ht + "-" + hk + "."
        if e.get("date_field") else
        "  SELECT-OPTIONS: s_" + hk_s + " FOR " + ht + "-" + hk + "."
    )

    code = (
        "*&---------------------------------------------------------------------*\n"
        "*& Report  Z_FETCH_" + ht + "\n"
        "*& Description: Fetch " + label + "\n"
        "*&---------------------------------------------------------------------*\n"
        "REPORT z_fetch_" + ht_l + ".\n\n"
        "TABLES: " + ht + ".\n\n"
        "SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.\n"
        + sel_screen + "\n"
        "SELECTION-SCREEN END OF BLOCK b1.\n\n"
        "TYPES:\n"
        "  BEGIN OF ty_result,\n"
        + htype + "\n"
        "  END OF ty_result.\n\n"
        "DATA:\n"
        "  lt_result    TYPE TABLE OF ty_result,\n"
        "  ls_result    TYPE ty_result,\n"
        "  lv_date_from TYPE d.\n\n"
        "INITIALIZATION.\n"
        "  lv_date_from = sy-datum - 30.\n\n"
        "START-OF-SELECTION.\n\n"
        "  SELECT " + hfields + "\n"
        "    FROM " + ht + "\n"
        "    INTO TABLE @lt_result\n"
        + ("    WHERE " + where + ".\n" if where else "    WHERE " + hk + " IN @s_" + hk_s + ".\n") +
        "\n"
        "  IF sy-subrc NE 0.\n"
        "    MESSAGE 'No " + hdesc + " records found.' TYPE 'I'.\n"
        "    RETURN.\n"
        "  ENDIF.\n\n"
        "END-OF-SELECTION.\n\n"
        "  WRITE: / '=== " + hdesc.upper() + " ==='.\n"
        "  LOOP AT lt_result INTO ls_result.\n"
        "    WRITE: / " + hwrite + ".\n"
        "  ENDLOOP.\n"
        "  WRITE: / 'Total Records:', lines( lt_result ).\n"
    )

    return (
        "\U0001F4DD **ABAP Report \u2014 " + label + "**\n\n"
        "Table: `" + ht + "` (" + hdesc + ")\n\n"
        "```abap\n" + code + "\n```\n\n"
        "**How to use:**\n"
        "1. Open **SE38** in SAP GUI\n"
        "2. Create program `Z_FETCH_" + ht + "`\n"
        "3. Paste code, Activate (`Ctrl+F3`), Execute (`F8`)\n\n"
        "*Adjust selection screen parameters as needed.*"
    )


def _abap_generic() -> str:
    supported = ", ".join(
        "`" + k.title() + "`" for k in _ABAP_ENTITIES.keys()
    )
    return (
        "\U0001F4DD **ABAP Code Generator**\n\n"
        "I can generate ABAP reports for:\n\n"
        + supported + "\n\n"
        "**Try:**\n"
        "- `Give me ABAP for Sales Orders`\n"
        "- `Write ABAP report for Purchase Orders and Items`\n"
        "- `ABAP code to fetch Billing Documents`\n"
        "- `Generate ABAP for General Ledger entries`\n"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# INTELLIGENT ABAP & RAP CODE GENERATION ENGINE  (v2 — new feature)
# Supports: ABAP Programs, CDS Views, RAP Models, OData Services, ABAP Classes
# ═══════════════════════════════════════════════════════════════════════════════

# ── Output-type detection ─────────────────────────────────────────────────────

def detect_abap_output_type(prompt: str) -> str:
    """
    Returns one of: 'cds_view' | 'rap_model' | 'odata_service' |
                    'abap_class' | 'abap_program'
    Priority: explicit keyword first, then infer from context.
    """
    t = prompt.lower()
    if any(k in t for k in ["cds view", "cds annotation", "eclipse cds", "define view", "define root view"]):
        return "cds_view"
    if any(k in t for k in ["rap model", "rap", "bdef", "behaviour definition", "behavior definition",
                              "managed scenario", "unmanaged scenario", "rap entity"]):
        return "rap_model"
    if any(k in t for k in ["odata service", "odata v4", "odata v2", "service binding",
                              "service definition", "expose as odata"]):
        return "odata_service"
    if any(k in t for k in ["abap class", "class-based", "oo abap", "class definition",
                              "create class", "method ", "local class"]):
        return "abap_class"
    # Default for SE38-style programs
    return "abap_program"


def is_advanced_abap_request(prompt: str) -> bool:
    """
    Detects advanced ABAP / RAP requests beyond simple SELECT reports.
    Triggers for CDS, RAP, OData, Financial/FM, Budget, commitment data etc.
    """
    t = prompt.lower()
    advanced_kws = [
        "cds view", "rap model", "rap ", "bdef", "behaviour definition",
        "odata service", "service binding", "service definition",
        "abap class", "class-based", "oo abap",
        "financial commitment", "commitment data", "funds management",
        "budget", "fm area", "grant", "fund center",
        "pooling", "pool from", "union", "combine",
        "process order", "production order", "maintenance order",
        "internal order", "profit center", "segment",
        "withholding tax", "asset accounting", "fixed asset",
        "batch input", "bdc", "legacy transfer",
        "enhancement spot", "badi implementation",
        "authorization check", "authority-check",
    ]
    return any(kw in t for kw in advanced_kws)


# ── Financial Commitment / Budget data — special pooling scenario ─────────────

def _generate_commitment_pooling_abap(prompt: str) -> str:
    """
    Generate ABAP program to pool financial commitment data from PO + Budget/FM.
    Uses: EKKO/EKPO (PO), FMIFIIT/FMIA (FM commitment), RPSCO/COSP (CO budget)
    """
    t = prompt.lower()
    output_type = detect_abap_output_type(prompt)

    if output_type == "cds_view":
        return _generate_commitment_cds(prompt)
    if output_type == "rap_model":
        return _generate_commitment_rap(prompt)

    # Default: ABAP Program (SE38)
    code = '''\
REPORT Z_COMMITMENT_POOL_REPORT.
*======================================================================
* Program  : Z_COMMITMENT_POOL_REPORT
* Purpose  : Pool Financial Commitment Data from PO and Budget/FM
* Author   : Generated by SAP CPI iFlow Intelligence Generator
* Date     : ''' + __import__('datetime').date.today().isoformat() + '''
* Modules  : MM (Purchase Orders) + FM (Funds Management / Budget)
* Tables   : EKKO, EKPO (PO), FMIFIIT (FM Commitments), RPSCO (CO Budget)
*
* IMPORTANT: Replace placeholder table/field names with your system\'s
*            custom equivalents if standard tables differ in your release.
*======================================================================

*----------------------------------------------------------------------
* Type declarations
*----------------------------------------------------------------------
TYPES:
  BEGIN OF ty_po_commitment,
    ebeln     TYPE ekko-ebeln,       " Purchase Order Number
    lifnr     TYPE ekko-lifnr,       " Vendor
    bukrs     TYPE ekko-bukrs,       " Company Code
    waers     TYPE ekko-waers,       " Currency
    ebelp     TYPE ekpo-ebelp,       " PO Item
    matnr     TYPE ekpo-matnr,       " Material
    txz01     TYPE ekpo-txz01,       " Short text
    menge     TYPE ekpo-menge,       " Quantity
    meins     TYPE ekpo-meins,       " Unit
    netpr     TYPE ekpo-netpr,       " Net Price
    netwr     TYPE ekpo-netwr,       " Net Value (commitment amount)
    eindt     TYPE ekpo-eindt,       " Delivery Date
    kostl     TYPE ekpo-kostl,       " Cost Centre
    prctr     TYPE ekpo-prctr,       " Profit Centre
    aufnr     TYPE ekpo-aufnr,       " Order
  END OF ty_po_commitment.

TYPES:
  BEGIN OF ty_fm_commitment,
    fikrs     TYPE fmifiit-fikrs,    " FM Area
    fipex     TYPE fmifiit-fipex,    " Commitment Item
    fonds     TYPE fmifiit-fonds,    " Fund
    farea     TYPE fmifiit-farea,    " Functional Area
    gjahr     TYPE fmifiit-gjahr,    " Fiscal Year
    wrttp     TYPE fmifiit-wrttp,    " Value Type
    wlges     TYPE fmifiit-wlges,    " Amount in Local Currency
    twaer     TYPE fmifiit-twaer,    " Currency
  END OF ty_fm_commitment.

TYPES:
  BEGIN OF ty_budget,
    objnr     TYPE rpsco-objnr,      " Object Number
    gjahr     TYPE rpsco-gjahr,      " Fiscal Year
    versn     TYPE rpsco-versn,      " Version
    wrttp     TYPE rpsco-wrttp,      " Value Type (01=Budget, 04=Commitment)
    beknz     TYPE rpsco-beknz,      " Debit/Credit Indicator
    wtg001    TYPE rpsco-wtg001,     " Period 1 Value
    wtg002    TYPE rpsco-wtg002,     " Period 2 Value
    wtg003    TYPE rpsco-wtg003,     " Period 3 Value
    wtg004    TYPE rpsco-wtg004,     " Period 4 Value
    wtg005    TYPE rpsco-wtg005,     " Period 5 Value
    wtg006    TYPE rpsco-wtg006,     " Period 6 Value
  END OF ty_budget.

TYPES:
  BEGIN OF ty_pool_result,
    source    TYPE c LENGTH 10,      " 'PO' or 'FM' or 'BUDGET'
    doc_no    TYPE char20,           " Document number
    item_no   TYPE char10,
    bukrs     TYPE bukrs,
    kostl     TYPE kostl,
    aufnr     TYPE aufnr,
    amount    TYPE wrbtr,
    currency  TYPE waers,
    gjahr     TYPE gjahr,
    period    TYPE monat,
    doc_type  TYPE char20,
    text      TYPE char60,
  END OF ty_pool_result.

*----------------------------------------------------------------------
* Internal tables
*----------------------------------------------------------------------
DATA: lt_po_com    TYPE TABLE OF ty_po_commitment,
      lt_fm_com    TYPE TABLE OF ty_fm_commitment,
      lt_budget    TYPE TABLE OF ty_budget,
      lt_pool      TYPE TABLE OF ty_pool_result,
      ls_pool      TYPE ty_pool_result.

*----------------------------------------------------------------------
* Selection Screen
*----------------------------------------------------------------------
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
  SELECT-OPTIONS: s_bukrs FOR ekko-bukrs DEFAULT \'1000\',
                  s_ebeln FOR ekko-ebeln,
                  s_kostl FOR ekpo-kostl.
  PARAMETERS:     p_gjahr TYPE gjahr DEFAULT sy-datum(4),
                  p_versn TYPE rpsco-versn DEFAULT \'0\'.
SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.
  PARAMETERS: p_show_po     TYPE c AS CHECKBOX DEFAULT \'X\',
              p_show_fm     TYPE c AS CHECKBOX DEFAULT \'X\',
              p_show_budget TYPE c AS CHECKBOX DEFAULT \'X\'.
SELECTION-SCREEN END OF BLOCK b2.

*----------------------------------------------------------------------
* Start of Selection
*----------------------------------------------------------------------
START-OF-SELECTION.

  PERFORM fetch_po_commitments.
  PERFORM fetch_fm_commitments.
  PERFORM fetch_budget_data.
  PERFORM pool_results.

END-OF-SELECTION.
  PERFORM display_results.

*----------------------------------------------------------------------
* FORM: Fetch PO Commitments (EKKO + EKPO)
*----------------------------------------------------------------------
FORM fetch_po_commitments.
  CHECK p_show_po = \'X\'.

  DATA: lt_ekko TYPE TABLE OF ekko,
        lt_ekpo TYPE TABLE OF ekpo,
        ls_po   TYPE ty_po_commitment.

  " Fetch PO headers
  SELECT ebeln lifnr bukrs waers
    FROM ekko
    INTO TABLE @lt_ekko
    WHERE bukrs IN @s_bukrs
      AND ebeln IN @s_ebeln
      AND loekz = \'\'.        " Not deleted

  IF sy-subrc NE 0 OR lt_ekko IS INITIAL.
    MESSAGE \'No PO header records found.\' TYPE \'I\'.
    RETURN.
  ENDIF.

  " Fetch PO items (commitment data)
  SELECT a~ebeln a~lifnr a~bukrs a~waers
         b~ebelp b~matnr b~txz01 b~menge b~meins
         b~netpr b~netwr b~eindt b~kostl b~prctr b~aufnr
    FROM ekko AS a
    INNER JOIN ekpo AS b ON b~ebeln = a~ebeln
    INTO CORRESPONDING FIELDS OF TABLE @lt_po_com
    FOR ALL ENTRIES IN @lt_ekko
    WHERE a~ebeln  = @lt_ekko-ebeln
      AND b~kostl IN @s_kostl
      AND b~loekz = \'\'        " Not deleted
      AND b~elikz = \'\'        " Delivery not completed
      AND b~netwr GT 0.        " Only open commitment values

  IF sy-subrc NE 0.
    MESSAGE \'No open PO commitment items found.\' TYPE \'I\'.
  ENDIF.
ENDFORM.

*----------------------------------------------------------------------
* FORM: Fetch FM Commitments (FMIFIIT — FM Actual/Commitment Line Items)
*----------------------------------------------------------------------
FORM fetch_fm_commitments.
  CHECK p_show_fm = \'X\'.

  " NOTE: FMIFIIT may not exist in all SAP releases.
  " Alternative: Use FM_GET_COMMITMENT_ITEMS function module or
  " table FMIT (FM: Transaction Data), FMICPO (FM: Commitment Postings)
  " Replace table/fields with your system\'s equivalents.

  SELECT fikrs fipex fonds farea gjahr wrttp wlges twaer
    FROM fmifiit                          " <-- Replace if needed
    INTO TABLE @lt_fm_com
    WHERE gjahr  =  @p_gjahr
      AND wrttp  =  \'54\'                 " 54 = Purchase Order Commitments
      AND fikrs  IN @s_bukrs.             " FM Area = Company Code (typical)

  IF sy-subrc NE 0.
    " Try alternative: FMICPO
    " SELECT ... FROM fmicpo INTO TABLE @lt_fm_com WHERE ...
    MESSAGE \'No FM commitment records found (check table FMIFIIT/FMICPO).\' TYPE \'I\'.
  ENDIF.
ENDFORM.

*----------------------------------------------------------------------
* FORM: Fetch Budget Data (RPSCO — CO Plan/Budget by Period)
*----------------------------------------------------------------------
FORM fetch_budget_data.
  CHECK p_show_budget = \'X\'.

  " RPSCO stores budget, plan and actuals by object/period.
  " wrttp: 41=Plan, 01=Budget, 04=Commitment, 11=Actuals
  " objnr: \'KS\' + KOKRS + KOSTL for cost centres

  SELECT objnr gjahr versn wrttp beknz
         wtg001 wtg002 wtg003 wtg004 wtg005 wtg006
    FROM rpsco
    INTO TABLE @lt_budget
    WHERE gjahr = @p_gjahr
      AND versn = @p_versn
      AND wrttp IN (\'01\', \'04\', \'41\')  " Budget, Commitment, Plan
      AND objnr LIKE \'KS%\'.               " Cost centre objects

  IF sy-subrc NE 0.
    MESSAGE \'No budget/plan records in RPSCO.\' TYPE \'I\'.
  ENDIF.
ENDFORM.

*----------------------------------------------------------------------
* FORM: Pool Results into unified internal table
*----------------------------------------------------------------------
FORM pool_results.
  DATA: ls_po  LIKE LINE OF lt_po_com,
        ls_fm  LIKE LINE OF lt_fm_com,
        ls_bud LIKE LINE OF lt_budget.

  " Pool PO Commitments
  LOOP AT lt_po_com INTO ls_po.
    CLEAR ls_pool.
    ls_pool-source   = \'PO\'.
    ls_pool-doc_no   = ls_po-ebeln.
    ls_pool-item_no  = ls_po-ebelp.
    ls_pool-bukrs    = ls_po-bukrs.
    ls_pool-kostl    = ls_po-kostl.
    ls_pool-aufnr    = ls_po-aufnr.
    ls_pool-amount   = ls_po-netwr.
    ls_pool-currency = ls_po-waers.
    ls_pool-gjahr    = p_gjahr.
    ls_pool-doc_type = \'PURCHASE ORDER\'.
    ls_pool-text     = ls_po-txz01.
    APPEND ls_pool TO lt_pool.
  ENDLOOP.

  " Pool FM Commitments
  LOOP AT lt_fm_com INTO ls_fm.
    CLEAR ls_pool.
    ls_pool-source   = \'FM\'.
    ls_pool-doc_no   = ls_fm-fipex.
    ls_pool-bukrs    = ls_fm-fikrs.
    ls_pool-amount   = ls_fm-wlges.
    ls_pool-currency = ls_fm-twaer.
    ls_pool-gjahr    = ls_fm-gjahr.
    ls_pool-doc_type = \'FM COMMITMENT\'.
    ls_pool-text     = |FM Area: { ls_fm-fikrs } Fund: { ls_fm-fonds }|.
    APPEND ls_pool TO lt_pool.
  ENDLOOP.

  " Pool Budget
  LOOP AT lt_budget INTO ls_bud.
    CLEAR ls_pool.
    ls_pool-source   = \'BUDGET\'.
    ls_pool-doc_no   = ls_bud-objnr.
    ls_pool-gjahr    = ls_bud-gjahr.
    ls_pool-amount   = ls_bud-wtg001 + ls_bud-wtg002 + ls_bud-wtg003
                     + ls_bud-wtg004 + ls_bud-wtg005 + ls_bud-wtg006.
    ls_pool-doc_type = SWITCH #( ls_bud-wrttp
                         WHEN \'01\' THEN \'BUDGET\'
                         WHEN \'04\' THEN \'CO COMMITMENT\'
                         WHEN \'41\' THEN \'PLAN\'
                         ELSE ls_bud-wrttp ).
    ls_pool-text     = |Object: { ls_bud-objnr } Ver: { ls_bud-versn }|.
    APPEND ls_pool TO lt_pool.
  ENDLOOP.
ENDFORM.

*----------------------------------------------------------------------
* FORM: Display Results
*----------------------------------------------------------------------
FORM display_results.
  DATA: ls_p   LIKE LINE OF lt_pool,
        lv_tot TYPE wrbtr.

  IF lt_pool IS INITIAL.
    WRITE: / \'No commitment or budget data found for the selection.\'.
    RETURN.
  ENDIF.

  " Sort by source then document number
  SORT lt_pool BY source doc_no item_no.

  WRITE: /1 \'Source\', 12 \'Document\', 30 \'Item\', 36 \'CoCd\',
           42 \'Cost Ctr\', 52 \'Amount\', 68 \'Curr\', 74 \'Fiscal Yr\',
           82 \'Type\', 102 \'Description\'.
  ULINE.

  LOOP AT lt_pool INTO ls_p.
    WRITE: /1  ls_p-source,
            12 ls_p-doc_no,
            30 ls_p-item_no,
            36 ls_p-bukrs,
            42 ls_p-kostl,
            52 ls_p-amount CURRENCY ls_p-currency,
            68 ls_p-currency,
            74 ls_p-gjahr,
            82 ls_p-doc_type,
           102 ls_p-text.
    lv_tot = lv_tot + ls_p-amount.
  ENDLOOP.

  ULINE.
  WRITE: / \'Total Commitment + Budget Exposure:\', lv_tot.
  WRITE: / \'Records:\', lines( lt_pool ).
ENDFORM.
'''
    header = (
        "📝 **ABAP Program — Financial Commitment Data Pool (PO + Budget/FM)**\n\n"
        "Modules: **MM** (Purchase Orders) | **FM** (Funds Management) | **CO** (Budget via RPSCO)\n\n"
        "Tables used: `EKKO`, `EKPO` (PO) · `FMIFIIT` (FM Commitments) · `RPSCO` (CO Budget/Plan)\n\n"
        "```abap\n" + code + "\n```\n\n"
        "**⚠ Table Notes:**\n"
        "- `FMIFIIT` — FM commitment line items; replace with `FMICPO` or `FMIT` if not available in your release\n"
        "- `RPSCO` — CO plan/budget totals; wrttp `01`=Budget, `04`=Commitment, `41`=Plan\n"
        "- `EKPO-KOSTL` — cost centre on PO item (may need to be populated via account assignment)\n\n"
        "**How to use:**\n"
        "1. Open **SE38** → Create program `Z_COMMITMENT_POOL_REPORT`\n"
        "2. Paste code → Activate (`Ctrl+F3`) → Execute (`F8`)\n"
        "3. Enter Company Code, Fiscal Year, and optional filters\n"
        "4. Check 'Show PO / FM / Budget' checkboxes as needed\n\n"
        "*Adjust table/field names to match your SAP release and custom objects.*"
    )
    return header


def _generate_commitment_cds(prompt: str) -> str:
    cds = '''\
@AbapCatalog.sqlViewName: 'ZCOMMIT_POOL_V'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'Financial Commitment Pool - PO + FM + Budget'
@VDM.viewType: #BASIC

define view Z_I_CommitmentPool
  as select from ekko as Header
  inner join ekpo as Item on Item.ebeln = Header.ebeln
{
  // ── Purchase Order Header ─────────────────────────────────────
  @EndUserText.label: 'PO Number'
  key Header.ebeln          as PurchaseOrder,

  @EndUserText.label: 'PO Item'
  key Item.ebelp            as PurchaseOrderItem,

  @EndUserText.label: 'Company Code'
  Header.bukrs              as CompanyCode,

  @EndUserText.label: 'Vendor'
  Header.lifnr              as Vendor,

  @EndUserText.label: 'Currency'
  @Semantics.currencyCode: true
  Header.waers              as Currency,

  // ── PO Item Commitment Data ───────────────────────────────────
  @EndUserText.label: 'Material'
  Item.matnr                as Material,

  @EndUserText.label: 'Short Text'
  Item.txz01                as ShortText,

  @EndUserText.label: 'Quantity'
  @Semantics.quantity.unitOfMeasure: 'UnitOfMeasure'
  Item.menge                as Quantity,

  Item.meins                as UnitOfMeasure,

  @EndUserText.label: 'Net Price'
  @Semantics.amount.currencyCode: 'Currency'
  Item.netpr                as NetPrice,

  @EndUserText.label: 'Commitment Amount (Net Value)'
  @Semantics.amount.currencyCode: 'Currency'
  Item.netwr                as CommitmentAmount,

  @EndUserText.label: 'Cost Centre'
  Item.kostl                as CostCentre,

  @EndUserText.label: 'Profit Centre'
  Item.prctr                as ProfitCentre,

  @EndUserText.label: 'Order'
  Item.aufnr                as InternalOrder,

  @EndUserText.label: 'Delivery Date'
  Item.eindt                as DeliveryDate,

  @EndUserText.label: 'Source Type'
  cast( 'PO' as abap.char(10) )  as CommitmentSource

  // TODO: UNION or ASSOCIATION to FM/Budget views
  // Add: association [0..*] to Z_I_FMCommitment as _FMCommitment
  //                  on _FMCommitment.DocumentNumber = $projection.PurchaseOrder
}
where Item.loekz = ''   -- Not deleted
  and Item.elikz = ''   -- Delivery not complete
  and Item.netwr > 0    -- Open commitment only
'''
    return (
        "🔷 **CDS View — Financial Commitment Pool (Eclipse-ready)**\n\n"
        "Save as: `Z_I_CommitmentPool` in ADT (Eclipse)\n\n"
        "```cds\n" + cds + "\n```\n\n"
        "**Next steps:**\n"
        "1. Create in **Eclipse ADT** → New ABAP Repository Object → Core Data Services → Data Definition\n"
        "2. Name: `Z_I_CommitmentPool`\n"
        "3. Add a second CDS `Z_I_FMCommitment` sourced from `FMIFIIT` and union/associate\n"
        "4. Expose via **Service Definition + Service Binding** for OData V4\n\n"
        "*Replace `FMIFIIT` with your FM commitment table if different in your release.*"
    )


def _generate_commitment_rap(prompt: str) -> str:
    rap = '''\
*=====================================================================
* RAP Model — Financial Commitment Pool
* Layer 1: CDS Root View Entity
*=====================================================================
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'RAP Root - Commitment Pool'

define root view entity Z_R_CommitmentPool
  as select from ekko as Header
  inner join ekpo as Item on Item.ebeln = Header.ebeln
{
  key Header.ebeln          as PurchaseOrder,
  key Item.ebelp            as PurchaseOrderItem,
      Header.bukrs          as CompanyCode,
      Header.lifnr          as Vendor,
      Header.waers          as Currency,
      Item.matnr            as Material,
      Item.txz01            as ShortText,
      Item.menge            as Quantity,
      Item.meins            as UnitOfMeasure,
      Item.netwr            as CommitmentAmount,
      Item.kostl            as CostCentre,
      Item.prctr            as ProfitCentre,
      Item.eindt            as DeliveryDate,
      -- Administrative fields for RAP
      Header.aedat          as LastChangedDate,
      cast(0 as abap.int4)  as LocalLastChangedAt
}
where Item.loekz = '' and Item.netwr > 0;

*=====================================================================
* Layer 2: Behaviour Definition (BDEF)
*=====================================================================
-- File: Z_R_CommitmentPool (Behaviour Definition)
managed;

define behavior for Z_R_CommitmentPool alias CommitmentPool
  implementation in class zbp_r_commitmentpool unique
  lock master
  authorization master ( instance )
  etag master LocalLastChangedAt
{
  -- Read-only RAP object (no CUD — commitment data is read-only)
  -- Enable standard operations:
  internal create;
  internal update;
  internal delete;

  -- Custom actions
  action ( features : instance ) refreshCommitments result [1] $self;

  -- Field control
  field ( readonly ) PurchaseOrder, PurchaseOrderItem;
  field ( mandatory ) CompanyCode;
}

*=====================================================================
* Layer 3: Behaviour Implementation Class (skeleton)
*=====================================================================
CLASS zbp_r_commitmentpool DEFINITION
  PUBLIC FINAL
  FOR BEHAVIOR OF Z_R_CommitmentPool.

  PUBLIC SECTION.
  PROTECTED SECTION.
  PRIVATE SECTION.
    METHODS:
      " Custom action: refresh commitment totals
      refreshCommitments FOR MODIFY
        IMPORTING keys FOR ACTION CommitmentPool~refreshCommitments
        RESULT result.
ENDCLASS.

CLASS zbp_r_commitmentpool IMPLEMENTATION.
  METHOD refreshCommitments.
    " TODO: Implement refresh logic — re-read EKKO/EKPO/FMIFIIT
    " and update commitment amounts if using custom persistence
  ENDMETHOD.
ENDCLASS.

*=====================================================================
* Layer 4: Service Definition
*=====================================================================
-- @EndUserText.label: 'Commitment Pool Service'
-- define service Z_SD_CommitmentPool {
--   expose Z_R_CommitmentPool as CommitmentPool;
-- }

*=====================================================================
* Layer 5: Service Binding (OData V4 UI)
*=====================================================================
-- Create via ADT: New → Service Binding
-- Binding Type: OData V4 - UI
-- Name: Z_SB_COMMITMENTPOOL_UI
-- Activate → Publish → Test in Fiori Preview
'''
    return (
        "🔶 **RAP Model — Financial Commitment Pool (Eclipse-ready)**\n\n"
        "Layers: Root CDS View Entity → Behaviour Definition → Implementation Class → Service Definition → Service Binding\n\n"
        "```abap\n" + rap + "\n```\n\n"
        "**How to deploy in Eclipse ADT:**\n"
        "1. Create **Data Definition** `Z_R_CommitmentPool` (Root View Entity)\n"
        "2. Create **Behaviour Definition** `Z_R_CommitmentPool` (Managed)\n"
        "3. Create **Behaviour Implementation** class `ZBP_R_COMMITMENTPOOL`\n"
        "4. Create **Service Definition** `Z_SD_CommitmentPool`\n"
        "5. Create **Service Binding** `Z_SB_COMMITMENTPOOL_UI` (OData V4 UI) → Activate\n\n"
        "*Adjust field mappings and add Fiori annotations as required.*"
    )


# ── CDS View Generator ────────────────────────────────────────────────────────

def generate_cds_view(prompt: str) -> str:
    """Generate an Eclipse-ready CDS View based on user prompt."""
    t = prompt.lower()
    entity_key = _detect_abap_entity(prompt)

    # Special cases
    if any(k in t for k in ["commitment", "budget", "funds management", "fm area"]):
        return _generate_commitment_cds(prompt)

    if not entity_key:
        return _cds_generic()

    e = _ABAP_ENTITIES[entity_key]
    ht = e["header_tbl"]
    it = e["item_tbl"]
    label = e["label"]
    hfields = e["header_fields"]
    ifields = e.get("item_fields", [])

    # Build field list
    def _cds_fields(fields, tbl, prefix=""):
        lines = []
        for f in fields:
            ann = ""
            if f in ("NETWR", "DMBTR", "WRBTR", "NETPR"):
                ann = "  @Semantics.amount.currencyCode: 'Currency'\n  "
            elif f in ("WAERS", "WAERK", "TWAER"):
                ann = "  @Semantics.currencyCode: true\n  "
            elif f in ("ERDAT", "BUDAT", "BLDAT", "AEDAT", "FKDAT", "BADAT"):
                ann = "  @Semantics.date: #DATE\n  "
            lines.append(f"  {ann}key {prefix}{tbl}.{f.lower():<20} as {f.title().replace('_','')}")
        return ",\n".join(lines)

    if it:
        cds_code = f'''\
@AbapCatalog.sqlViewName: 'Z{ht[:12]}_V'
@AbapCatalog.compiler.compareFilter: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: \'CDS View — {label}\'
@VDM.viewType: #BASIC

define view Z_I_{ht}_{it}
  as select from {ht} as Header
  inner join   {it} as Item on Item.{e["join_key"].lower()} = Header.{e["header_key"].lower()}
{{
  // ── {e["header_desc"]} ─────────────────────────────────────
{_cds_fields(hfields, "Header", "")},

  // ── {e["item_desc"]} ────────────────────────────────────────
{_cds_fields(ifields, "Item", "")}
}}
'''
    else:
        cds_code = f'''\
@AbapCatalog.sqlViewName: 'Z{ht[:14]}_V'
@AbapCatalog.compiler.compareFilter: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: \'CDS View — {label}\'
@VDM.viewType: #BASIC

define view Z_I_{ht}
  as select from {ht}
{{
  // ── {e["header_desc"]} ─────────────────────────────────────
{_cds_fields(hfields, ht, "")}
}}
'''

    return (
        f"🔷 **CDS View — {label} (Eclipse-ready)**\n\n"
        f"```cds\n{cds_code}\n```\n\n"
        "**How to create in Eclipse ADT:**\n"
        "1. Right-click Package → New → Other → Core Data Services → Data Definition\n"
        f"2. Name: `Z_I_{ht}{'_' + it if it else ''}`, Description: `{label}`\n"
        "3. Paste code → Activate (`Ctrl+F3`)\n"
        "4. To expose as OData: add Service Definition + Service Binding\n\n"
        "*Add `@UI` and `@Consumption` annotations for Fiori Elements apps.*"
    )


def _cds_generic() -> str:
    supported = ", ".join("`" + k.title() + "`" for k in _ABAP_ENTITIES.keys())
    return (
        "🔷 **CDS View Generator**\n\n"
        "I can generate CDS Views for:\n\n"
        f"{supported}\n\n"
        "**Try:**\n"
        "- `CDS View for Purchase Orders`\n"
        "- `Generate CDS view for Sales Orders and Items`\n"
        "- `Eclipse CDS View for Financial Commitment data`\n"
        "- `Define root view for Material Master`\n"
    )


# ── RAP Model Generator ───────────────────────────────────────────────────────

def generate_rap_model(prompt: str) -> str:
    """Generate a full RAP model skeleton."""
    t = prompt.lower()

    if any(k in t for k in ["commitment", "budget", "funds management"]):
        return _generate_commitment_rap(prompt)

    entity_key = _detect_abap_entity(prompt)
    if not entity_key:
        return _rap_generic()

    e = _ABAP_ENTITIES[entity_key]
    ht = e["header_tbl"]
    label = e["label"]
    hfields = e["header_fields"]
    hk = e["header_key"]

    fields_block = "\n".join(
        f"      {ht.lower()}.{f.lower():<20} as {f.title().replace('_','')},"
        for f in hfields
    ).rstrip(",") + ","

    rap = f'''\
*=====================================================================
* RAP Model — {label}
* Table: {ht}
*=====================================================================

-- ── 1. Root View Entity ───────────────────────────────────────
@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'RAP Root — {label}'

define root view entity Z_R_{ht}
  as select from {ht}
{{
  key {ht.lower()}.{hk.lower():<20} as {hk.title()},
{fields_block}
      cast(0 as abap.int4)  as LocalLastChangedAt
}}

-- ── 2. Behaviour Definition ───────────────────────────────────
-- (Create file Z_R_{ht} as Behaviour Definition)
managed;
define behavior for Z_R_{ht} alias {ht}
  implementation in class ZBP_R_{ht} unique
  lock master
  authorization master ( instance )
  etag master LocalLastChangedAt
{{
  create; update; delete;
  field ( readonly ) {hk};
  field ( mandatory ) {hfields[1] if len(hfields) > 1 else hk};
}}

-- ── 3. Service Definition ──────────────────────────────────────
-- define service Z_SD_{ht} {{
--   expose Z_R_{ht} as {ht};
-- }}

-- ── 4. Service Binding (OData V4 UI) ──────────────────────────
-- Name: Z_SB_{ht}_UI
-- Binding Type: OData V4 - UI
-- Activate → Publish → Test in Fiori Preview
'''
    return (
        f"🔶 **RAP Model — {label} (Eclipse-ready)**\n\n"
        f"Table: `{ht}` | Key: `{hk}`\n\n"
        f"```abap\n{rap}\n```\n\n"
        "**Deployment steps in Eclipse ADT:**\n"
        f"1. Data Definition: `Z_R_{ht}` (Root View Entity)\n"
        f"2. Behaviour Definition: `Z_R_{ht}` (Managed)\n"
        f"3. Behaviour Implementation: `ZBP_R_{ht}`\n"
        f"4. Service Definition: `Z_SD_{ht}`\n"
        f"5. Service Binding: `Z_SB_{ht}_UI` (OData V4 UI) → Activate\n\n"
        "*Add `@UI.lineItem`, `@UI.selectionField` annotations for Fiori Elements.*"
    )


def _rap_generic() -> str:
    supported = ", ".join("`" + k.title() + "`" for k in _ABAP_ENTITIES.keys())
    return (
        "🔶 **RAP Model Generator**\n\n"
        "I can generate RAP Models (Root View Entity + BDEF + Implementation) for:\n\n"
        f"{supported}\n\n"
        "**Try:**\n"
        "- `RAP model for Purchase Orders`\n"
        "- `Generate RAP entity for Sales Orders`\n"
        "- `RAP model with OData V4 for Material Master`\n"
    )


# ── ABAP Class Generator ──────────────────────────────────────────────────────

def generate_abap_class(prompt: str) -> str:
    """Generate a class-based ABAP program."""
    entity_key = _detect_abap_entity(prompt)
    t = prompt.lower()

    if not entity_key:
        return _abap_class_generic()

    e = _ABAP_ENTITIES[entity_key]
    ht = e["header_tbl"]
    it = e["item_tbl"]
    label = e["label"]
    hfields = e["header_fields"]
    hk = e["header_key"]
    hk_s = hk.lower()[:6]

    class_code = f'''\
*&---------------------------------------------------------------------*
*& Class-Based ABAP — {label}
*& SE24 / ADT — Local class for SE38 or global class in SE24
*&---------------------------------------------------------------------*
CLASS zcl_{ht.lower()}_handler DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    TYPES:
      tt_{ht.lower()} TYPE TABLE OF {ht} WITH EMPTY KEY.

    "! Fetch {label} records
    "! @parameter iv_{hk_s}  | Document number (optional, ranges)
    "! @parameter iv_bukrs   | Company Code filter
    "! @parameter rt_result  | Result table
    METHODS:
      fetch
        IMPORTING iv_{hk_s} TYPE {ht}-{hk} OPTIONAL
                  iv_bukrs  TYPE bukrs      OPTIONAL
        RETURNING VALUE(rt_result) TYPE tt_{ht.lower()},

      display_alv
        IMPORTING it_data TYPE tt_{ht.lower()}.

  PRIVATE SECTION.
    METHODS:
      build_fieldcat
        IMPORTING it_data TYPE tt_{ht.lower()}
        RETURNING VALUE(rt_fcat) TYPE lvc_t_fcat.

ENDCLASS.

CLASS zcl_{ht.lower()}_handler IMPLEMENTATION.

  METHOD fetch.
    " Fetch {label} from {ht}
    IF iv_{hk_s} IS NOT INITIAL.
      SELECT *
        FROM {ht}
        INTO TABLE @rt_result
        WHERE {hk} = @iv_{hk_s}.
    ELSEIF iv_bukrs IS NOT INITIAL.
      SELECT *
        FROM {ht}
        INTO TABLE @rt_result
        WHERE {"bukrs" if "BUKRS" in hfields else hk} = @iv_bukrs
        UP TO 1000 ROWS.
    ELSE.
      SELECT *
        FROM {ht}
        INTO TABLE @rt_result
        UP TO 500 ROWS.
    ENDIF.

    IF sy-subrc NE 0.
      MESSAGE |No {label} records found.| TYPE \'S\'.
    ENDIF.
  ENDMETHOD.

  METHOD display_alv.
    " ALV display
    DATA: lo_alv   TYPE REF TO cl_salv_table,
          lo_funcs TYPE REF TO cl_salv_functions,
          lx_err   TYPE REF TO cx_salv_msg.

    TRY.
        cl_salv_table=>factory(
          IMPORTING r_salv_table = lo_alv
          CHANGING  t_table      = it_data ).

        lo_funcs = lo_alv->get_functions( ).
        lo_funcs->set_all( abap_true ).

        lo_alv->get_columns( )->set_optimize( abap_true ).
        lo_alv->display( ).

      CATCH cx_salv_msg INTO lx_err.
        MESSAGE lx_err TYPE \'E\'.
    ENDTRY.
  ENDMETHOD.

  METHOD build_fieldcat.
    " Field catalog builder (optional override)
  ENDMETHOD.

ENDCLASS.

*&---------------------------------------------------------------------*
*& SE38 Executable Program using the class above
*&---------------------------------------------------------------------*
REPORT z_{ht.lower()}_oo_report.

SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
  PARAMETERS: p_{hk_s} TYPE {ht}-{hk} OPTIONAL,
              p_bukrs  TYPE bukrs DEFAULT \'1000\'.
SELECTION-SCREEN END OF BLOCK b1.

START-OF-SELECTION.
  DATA(lo_handler) = NEW zcl_{ht.lower()}_handler( ).
  DATA(lt_result)  = lo_handler->fetch(
    iv_{hk_s} = p_{hk_s}
    iv_bukrs  = p_bukrs ).
  lo_handler->display_alv( lt_result ).
'''
    return (
        f"📝 **ABAP Class — {label} (SE24 / ADT-ready)**\n\n"
        f"Table: `{ht}` | Class: `ZCL_{ht}_HANDLER`\n\n"
        f"```abap\n{class_code}\n```\n\n"
        "**How to use:**\n"
        f"1. Create global class `ZCL_{ht}_HANDLER` in SE24 (or use local class in SE38)\n"
        f"2. Create program `Z_{ht}_OO_REPORT` in SE38\n"
        "3. Activate and test with `F8`\n\n"
        "*Extend with `BAPI`, `RFC`, or `OData` calls as needed.*"
    )


def _abap_class_generic() -> str:
    return (
        "📝 **ABAP Class Generator**\n\n"
        "I can generate class-based ABAP programs for standard SAP entities.\n\n"
        "**Try:**\n"
        "- `ABAP class for Sales Orders`\n"
        "- `OO ABAP class for Purchase Orders`\n"
        "- `Class-based ABAP for Material Master`\n"
    )


# ── Master ABAP dispatcher (enhanced) ────────────────────────────────────────

def generate_abap_enhanced(prompt: str) -> str:
    """
    Enhanced ABAP dispatcher that routes to:
    - CDS View generator
    - RAP Model generator
    - ABAP Class generator
    - Commitment/Budget pooling program
    - Standard ABAP report (original)
    """
    output_type = detect_abap_output_type(prompt)
    t = prompt.lower()

    # Financial commitment pooling — special complex scenario
    if any(k in t for k in ["commitment", "budget pool", "pooling", "funds management",
                              "fm commitment", "po and budget", "budget and po"]):
        if output_type == "cds_view":
            return _generate_commitment_cds(prompt)
        elif output_type == "rap_model":
            return _generate_commitment_rap(prompt)
        else:
            return _generate_commitment_pooling_abap(prompt)

    if output_type == "cds_view":
        return generate_cds_view(prompt)
    elif output_type == "rap_model":
        return generate_rap_model(prompt)
    elif output_type == "abap_class":
        return generate_abap_class(prompt)
    elif output_type == "odata_service":
        # Generate RAP model (includes OData service binding)
        return generate_rap_model(prompt)
    else:
        # Standard ABAP program (original engine)
        return generate_abap(prompt)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONAL CONSULTING KNOWLEDGE BASE ENHANCEMENT  (v2 — new feature)
# Deep functional + configuration knowledge for SAP modules
# ═══════════════════════════════════════════════════════════════════════════════

_FUNCTIONAL_KB_EXTENDED = {

    # ── Process Order ──────────────────────────────────────────────────────────
    "process order": (
        "**Process Order — SAP PP/PI**\n\n"
        "A **Process Order** in SAP is used in Process Industries (PI) to plan, execute, "
        "and confirm the production of materials using a process (recipe-based) rather than "
        "a routing. It is the PI equivalent of a Production Order in discrete manufacturing.\n\n"
        "**Business Use Case:**\n"
        "Used in industries like Pharmaceuticals, Food & Beverage, Chemicals, and Cosmetics "
        "where production follows defined recipes (master recipes) with phases and operations.\n\n"
        "**End-to-End Process Flow:**\n"
        "1. **Demand Creation** — MRP/MPS creates Planned Orders based on demand\n"
        "2. **Order Creation** — Convert Planned Order to Process Order (COR1/COR2) or create directly\n"
        "3. **Material Availability Check** — System checks stock/requirements\n"
        "4. **Order Release** — Release order (COR2); triggers batch record, WM transfer orders\n"
        "5. **Goods Issue** — Issue components to order (MIGO — Mvt Type 261)\n"
        "6. **Phase/Operation Confirmation** — Record actual quantities, times, yield (COR6/COR6N)\n"
        "7. **Goods Receipt** — Post header material to stock (MIGO — Mvt Type 101)\n"
        "8. **Order Settlement** — Settle variances to cost objects (CO88/KO8G)\n"
        "9. **Order Completion** — Set TECO/CLSD status (COR3)\n\n"
        "**Integration:**\n"
        "- **PP** — Master Recipe (C201), Resources, Capacity Planning (CM01)\n"
        "- **MM** — Bill of Materials (CS01/CS02), Material Master, Goods Movements (MIGO)\n"
        "- **FI** — Automatic account determination for GI/GR/Settlement postings\n"
        "- **CO** — Order cost collection; variance calculation; settlement to cost centre/product cost\n\n"
        "**Key Configuration:**\n"
        "- Order Type (OPJH) — defines settlement profile, status management\n"
        "- Plant Parameters (OPL8) — default values per plant\n"
        "- Number Ranges (OPJJ) — process order number assignment\n"
        "- Scheduling Parameters (OPU3) — scheduling type, float times\n"
        "- Availability Check (OPJK) — checking rule for components\n\n"
        "**Transaction Codes:**\n"
        "`COR1` Create · `COR2` Change · `COR3` Display · `COR6N` Confirm · "
        "`COOIS` Order Information System · `MF60` Missing Parts · `CO88` Settlement\n\n"
        "**Key Tables:**\n"
        "`AUFK` (Order Master) · `AFKO` (Order Header PP) · `AFPO` (Order Item) · "
        "`AFVC` (Order Operations) · `RESB` (Component Requirements) · `AUFM` (Goods Movements)"
    ),

    # ── Production Order ───────────────────────────────────────────────────────
    "production order": (
        "**Production Order — SAP PP (Discrete Manufacturing)**\n\n"
        "A **Production Order** defines what is to be produced, in what quantity, at what time, "
        "and with what resources. Used in discrete manufacturing with routing-based operations.\n\n"
        "**End-to-End Flow:**\n"
        "1. MRP creates Planned Orders → 2. Convert to Production Order (CO01/CO40) → "
        "3. Component availability check → 4. Order Release (CO02) → "
        "5. Goods Issue components (MIGO 261) → 6. Confirmations (CO11N) → "
        "7. Goods Receipt finished product (MIGO 101) → 8. Settlement (CO88)\n\n"
        "**Integration:** PP (Routing/BOM) · MM (Materials/GI/GR) · FI (Auto postings) · CO (Cost collection)\n\n"
        "**Transactions:** `CO01` Create · `CO02` Change · `CO03` Display · `CO11N` Confirm · "
        "`COOIS` Information System · `CO88` Period-end settlement\n\n"
        "**Tables:** `AUFK` · `AFKO` · `AFPO` · `AFVC` · `RESB` · `AFRU` (Confirmations)"
    ),

    # ── Internal Order ────────────────────────────────────────────────────────
    "internal order": (
        "**Internal Order — SAP CO**\n\n"
        "An **Internal Order** in SAP CO is used to plan, collect, and settle costs for "
        "a specific internal task, project, or event (e.g., trade fair, office renovation).\n\n"
        "**Types:** Overhead Orders, Investment Orders (linked to Assets), Accrual Orders, "
        "Revenue Orders.\n\n"
        "**Flow:** Create (KO01) → Budget (KO22) → Post costs → Monitor (KO03/KKBC_ORD) "
        "→ Settlement (KO88/KO8G) → Complete (TECO) → Archive\n\n"
        "**Integration:** FI (postings via account assignment) · AA (investment orders → AuC) "
        "· PS (linked to WBS elements) · MM (purchasing with order account assignment)\n\n"
        "**Configuration:** Order Type (KOT2_OPA) · Settlement Profile (OKO7) · "
        "Budget Profile (OKOB) · Number Ranges (KONK)\n\n"
        "**Transactions:** `KO01` Create · `KO02` Change · `KO03` Display · `KO22` Budget · "
        "`KO88` Settlement · `S_ALR_87012993` Order Budget Report\n\n"
        "**Tables:** `AUFK` (Order Master) · `COSP` (CO: Plan Totals) · `COSS` (CO: Actuals)"
    ),

    # ── Financial Commitment / Budget ─────────────────────────────────────────
    "financial commitment": (
        "**Financial Commitment & Budget — SAP FM/CO**\n\n"
        "**Commitment** in SAP tracks financial obligations created before actual expenditure. "
        "Types: Purchase Requisition (PR), Purchase Order (PO), Funds Reservation, Earmarked Funds.\n\n"
        "**Funds Management (FM) Integration:**\n"
        "FM controls budget consumption at the FM account assignment level "
        "(FM Area + Fund + Functional Area + Commitment Item). "
        "When a PO is created, FM checks available budget (Availability Control — AVC).\n\n"
        "**Commitment Flow:**\n"
        "PR Created → PR Commitment posted to FM → PR converted to PO → "
        "PO Commitment replaces PR Commitment → GR posted → PO Commitment relieved → "
        "Invoice (MIRO) → Actuals posted\n\n"
        "**Key Tables:** `FMIFIIT` (FM Line Items) · `FMICPO` (FM Commitment Postings) · "
        "`RPSCO` (CO Budget/Plan Totals) · `EKKO/EKPO` (PO) · `EBAN` (PR) · `FMIOI` (FM Obligations)\n\n"
        "**Transactions:** `FMRP_RW_BUDCON` Budget Consumption Report · `FMEDD` Earmarked Funds · "
        "`ME2M` PO by Material · `S_P00_07000134` FM Budget Overview\n\n"
        "**ABAP/RAP Access:** Use `Z_COMMITMENT_POOL_REPORT` or ask for CDS/RAP model generation"
    ),

    # ── Vendor Invoice Verification ───────────────────────────────────────────
    "vendor invoice": (
        "**Vendor Invoice Verification — SAP MM/FI (MIRO)**\n\n"
        "The 3-way match process: PO → Goods Receipt → Vendor Invoice.\n\n"
        "**Process Flow:**\n"
        "1. Vendor sends invoice → 2. Accounts Payable enters via MIRO → "
        "3. System compares PO price/quantity with GR → 4. Tolerances checked → "
        "5. If within tolerance: post automatically → 6. If outside: block for review (MRBR) → "
        "7. Payment via F110 (Automatic Payment Program)\n\n"
        "**Account Postings (3-way match):**\n"
        "- Vendor account: Credit (liability)\n"
        "- GR/IR clearing: Debit (clears GR posting)\n"
        "- Tax account: Debit (if applicable)\n\n"
        "**Transactions:** `MIRO` Enter Invoice · `MIR4` Display Invoice · "
        "`MRBR` Release Blocked Invoices · `MR8M` Cancel Invoice · `F110` Payment Run\n\n"
        "**Tables:** `RBKP` (Invoice Header) · `RSEG` (Invoice Items) · `BKPF/BSEG` (FI Documents)"
    ),

    # ── Asset Accounting ──────────────────────────────────────────────────────
    "asset accounting": (
        "**Asset Accounting — SAP FI-AA**\n\n"
        "Manages the lifecycle of fixed assets: acquisition, depreciation, transfer, retirement.\n\n"
        "**Key Processes:**\n"
        "- **Acquisition:** Direct (F-90), via PO (MIGO), via internal order settlement\n"
        "- **Depreciation:** Planned automatically via depreciation keys (AFA); posted via AFAB\n"
        "- **Transfer:** Asset to Asset (ABUMN), or partial transfer\n"
        "- **Retirement:** Scrapping (ABAVN), Sale (F-92)\n"
        "- **Year-End Closing:** AJAB (fiscal year change), AJRW (fiscal year reopen)\n\n"
        "**Integration:** FI (G/L postings) · MM (PO-based acquisition) · "
        "CO (Internal Order → AuC → Asset capitalization) · PS (Investment projects)\n\n"
        "**Configuration:** Chart of Depreciation (OAOB) · Depreciation Areas · "
        "Asset Classes (OAOA) · Account Determination (AO90)\n\n"
        "**Transactions:** `AS01` Create Asset · `AS02` Change · `AS03` Display · "
        "`AFAB` Depreciation Run · `AW01N` Asset Explorer · `S_ALR_87011990` Asset Register\n\n"
        "**Tables:** `ANLA` (Asset Master) · `ANLZ` (Asset Time-Dep.) · `ANLC` (Asset Value Fields)"
    ),

    # ── Profit Center ────────────────────────────────────────────────────────
    "profit center": (
        "**Profit Center Accounting — SAP CO-PCA**\n\n"
        "Profit Centers represent internal organizational units for P&L reporting. "
        "In S/4HANA, profit center data is stored in the Universal Journal (ACDOCA).\n\n"
        "**Configuration:** Profit Center Standard Hierarchy (KCH1) · "
        "Dummy Profit Center (per controlling area) · Assignment to cost centres/materials/orders\n\n"
        "**Transactions:** `KE51` Create Profit Center · `KE52` Change · "
        "`KE5Z` Profit Center Accounting Report · `2KEE` Profit Center Planning\n\n"
        "**Tables (S/4HANA):** `CEPC` (Profit Center Master) · `ACDOCA` (Universal Journal)"
    ),

    # ── MRP ───────────────────────────────────────────────────────────────────
    "material requirements planning": (
        "**Material Requirements Planning (MRP) — SAP PP**\n\n"
        "MRP calculates material requirements based on demand (sales orders, forecasts, planned independent requirements) "
        "and creates procurement proposals (Planned Orders, Purchase Requisitions, Schedule Lines).\n\n"
        "**MRP Types (MARC-DISMM):**\n"
        "- `PD` — MRP (deterministic)\n"
        "- `VB` — Reorder Point Planning\n"
        "- `VV` — Forecast-based Planning\n"
        "- `M0` — No MRP\n\n"
        "**MRP Run:** MD01N (Total MRP) · MD02 (Single Item Multi-Level) · MD03 (Single Item Single-Level)\n\n"
        "**MRP Evaluations:** MD04 (Stock/Requirements List) · MD07 (Collective Stock/Reqmts) · "
        "MD06 (Exception Messages)\n\n"
        "**Integration:** PP (Planned Orders) · MM (PRs → POs) · SD (Sales Orders as demand) · "
        "WM (Transfer Orders from planned orders)\n\n"
        "**Tables:** `PLAF` (Planned Orders) · `MDKP` (MRP Document Header) · "
        "`MDTB` (MRP Table) · `RESB` (Reservations)"
    ),

    # ── WM / EWM ────────────────────────────────────────────────────────────
    "warehouse management": (
        "**Warehouse Management (WM/EWM) — SAP MM**\n\n"
        "WM manages stock at storage bin level within a warehouse. "
        "EWM (Extended Warehouse Management) is the advanced solution on SAP S/4HANA.\n\n"
        "**WM Process:** Transfer Order (TO) creation → Confirmation → Stock update at bin level\n\n"
        "**Key WM Transactions:** `LT01` Create TO · `LT0A` Auto TO · `LT1A` Confirm TO · "
        "`LS26` Bin Stock Overview · `LS24` Storage Bin Inventory · `LI01` Physical Inventory WM\n\n"
        "**EWM Process (S/4HANA):** Warehouse Request → Warehouse Task → "
        "Warehouse Order → Confirmation\n\n"
        "**Configuration (WM):** Warehouse Number → Storage Type → Storage Section → Storage Bin\n\n"
        "**Tables:** `LGPLA` (Bins) · `LQUA` (Quants/Stock) · `LTAP` (TO Items) · `LTBP` (TO Header)"
    ),

    # ── SD Order to Cash ──────────────────────────────────────────────────────
    "order to cash": (
        "**Order to Cash (OTC) — SAP SD End-to-End Process**\n\n"
        "**Process Flow:**\n"
        "1. **Pre-Sales:** Inquiry (VA11) → Quotation (VA21)\n"
        "2. **Sales Order:** Created via VA01. ATP check, credit check, pricing determination\n"
        "3. **Delivery:** VL01N — picks, packs, GI (movement type 601)\n"
        "4. **Billing:** VF01 — generates billing document and FI invoice\n"
        "5. **Payment:** Customer pays → incoming payment cleared in FI (F-28)\n\n"
        "**Integration:** SD (pricing, availability) · MM (stock reduction) · "
        "FI (revenue, AR posting) · CO (profitability analysis — CO-PA)\n\n"
        "**Transactions:** `VA01` Create SO · `VL01N` Create Delivery · `VF01` Create Billing · "
        "`VKM1` Blocked SD Documents (credit) · `VF04` Billing Due List\n\n"
        "**Tables:** `VBAK` (SO Header) · `VBAP` (SO Items) · `LIKP/LIPS` (Delivery) · "
        "`VBRK/VBRP` (Billing) · `KNA1` (Customer)"
    ),

    # ── Procure to Pay ────────────────────────────────────────────────────────
    "procure to pay": (
        "**Procure to Pay (P2P) — SAP MM/FI End-to-End**\n\n"
        "**Process Flow:**\n"
        "1. **Purchase Requisition (PR):** ME51N — department requests material/service\n"
        "2. **Source of Supply:** ME01 (Source List), ME11 (Info Record), ME31K (Contract)\n"
        "3. **Purchase Order (PO):** ME21N — approved PR converted to PO\n"
        "4. **Goods Receipt (GR):** MIGO (Mvt 101) — stock updated, GR/IR clearing posted\n"
        "5. **Invoice Verification (IV):** MIRO — 3-way match; vendor liability posted\n"
        "6. **Payment:** F110 (Auto Payment Program) — outgoing payment to vendor\n\n"
        "**Integration:** MM · FI/AP · CO (account assignment) · FM (commitment control)\n\n"
        "**Key Transactions:** `ME51N` PR · `ME21N` PO · `MIGO` GR/GI · `MIRO` Invoice · "
        "`F110` Payment · `MRBR` Release Blocked Invoices"
    ),

    # ── SAP Activate Methodology ──────────────────────────────────────────────
    "sap activate": (
        "**SAP Activate Methodology**\n\n"
        "SAP Activate is SAP's agile project methodology for implementing S/4HANA. "
        "It combines Best Practices, guided configuration, and agile sprints.\n\n"
        "**Six Phases:**\n"
        "1. **Discover** — Evaluate SAP solution fit; Trial system; Activate best practices\n"
        "2. **Prepare** — Project setup; system provisioning; learning paths\n"
        "3. **Explore** — Fit-to-Standard workshops; confirm scope; delta configuration\n"
        "4. **Realize** — Agile build sprints; configuration; integration testing; data migration\n"
        "5. **Deploy** — Go-live preparation; cutover; end-user training; UAT sign-off\n"
        "6. **Run** — Hypercare; support transition; continuous improvement\n\n"
        "**Deployment options:** Cloud (Greenfield) · System Conversion (Brownfield) · "
        "Selective Data Transition (Bluefield)\n\n"
        "**Tools:** SAP Readiness Check · Fit/Gap Analysis · LTMC (data migration) · "
        "SAP Central Business Configuration (for S/4HANA Cloud)"
    ),

    # ── Batch Management ──────────────────────────────────────────────────────
    "batch management": (
        "**Batch Management — SAP MM/PP/QM**\n\n"
        "Batch Management enables tracking of materials at batch level (lot number). "
        "Required in Pharma, Food, Chemical industries for traceability and shelf-life management.\n\n"
        "**Batch Types:** Internal (system-assigned) · External (vendor batch) · Customer batch\n\n"
        "**Key Features:** Shelf Life (MHD/BBD) · Batch Classification (CT04) · "
        "Batch Where-Used (MB56) · Batch Search Strategy (COB1)\n\n"
        "**Configuration:** Batch Level (plant/material/client — OMCE) · "
        "Activate batch management per material (Material Master — MM01 → Classification)\n\n"
        "**Transactions:** `MSC1N` Create Batch · `MB52` Batch Stock · `MB56` Batch Where-Used · "
        "`COB1` Batch Search Strategy · `MBC1` Batch Classification\n\n"
        "**Tables:** `MCH1` (Batch Master) · `MCHB` (Batch Stock) · `MCHA` (Batch Classification)"
    ),

    # ── ABAP Development — CDS Views ─────────────────────────────────────────
    "what is a cds view": (
        "**CDS View (Core Data Services) — SAP ABAP**\n\n"
        "A CDS View is a virtual data model defined using DDL (Data Definition Language) "
        "in Eclipse ADT. It abstracts database tables into semantically rich business objects.\n\n"
        "**Types:** Basic Views · Composite Views · Consumption Views · Interface Views\n\n"
        "**Annotations:** `@AbapCatalog` (DB view) · `@Semantics` (field semantics) · "
        "`@UI` (Fiori Elements layout) · `@VDM.viewType` (VDM classification) · "
        "`@OData.publish: true` (auto-publish as OData)\n\n"
        "**Advantages over DB tables:** Joins at select time · Calculated fields · "
        "Associations (lazy navigation) · Annotations for UI/OData auto-generation\n\n"
        "**Usage:** As data source for RAP models · Fiori Elements apps · "
        "Analytical queries · Custom OData services\n\n"
        "**Transaction:** Eclipse ADT only (no SE38/SE80 equivalent)\n\n"
        "**Example:** `define view Z_I_PurchaseOrder as select from ekko inner join ekpo...`"
    ),

    # ── RAP ───────────────────────────────────────────────────────────────────
    "what is rap": (
        "**RAP — ABAP RESTful Application Programming Model**\n\n"
        "RAP is the modern ABAP development framework for building SAP Fiori apps and OData V4 services. "
        "It replaces BOPF (Business Object Processing Framework).\n\n"
        "**RAP Architecture Layers:**\n"
        "1. **Data Model** — CDS Root View Entity (define root view entity)\n"
        "2. **Behaviour Definition (BDEF)** — Define CRUD + actions\n"
        "3. **Behaviour Implementation** — ABAP class with business logic\n"
        "4. **Service Definition** — Expose entities\n"
        "5. **Service Binding** — OData V2/V4 endpoint; Fiori Preview\n\n"
        "**Managed vs Unmanaged:**\n"
        "- Managed: Framework handles CRUD + draft → developer only writes business logic\n"
        "- Unmanaged: Developer implements all CRUD manually (legacy tables)\n\n"
        "**Used for:** Custom Fiori Apps · Side-by-side extensions · S/4HANA Cloud extensions · "
        "BTP ABAP Environment apps\n\n"
        "**Tools:** Eclipse ADT · SAP BTP ABAP Environment · S/4HANA on-premise 1909+"
    ),

    # ── Plan to Produce ───────────────────────────────────────────────────────
    "plan to produce": (
        "**Plan to Produce — SAP PP End-to-End**\n\n"
        "**Process Flow:**\n"
        "1. **Demand Planning:** Forecast (DP) or Sales Orders\n"
        "2. **Production Planning (SOP/MPS):** High-level planning → MPS run\n"
        "3. **MRP Run:** MD01N → creates Planned Orders / PR for externally procured components\n"
        "4. **Production Order:** CO01 — convert planned order; BOM exploded; routing attached\n"
        "5. **Material Availability:** Check components; trigger procurement if needed\n"
        "6. **Production Execution:** Goods Issue (MIGO 261); Operation Confirmations (CO11N)\n"
        "7. **Goods Receipt:** Post finished goods to stock (MIGO 101)\n"
        "8. **Order Closing:** Settlement (CO88); TECO/CLSD status\n\n"
        "**Integration:** PP · MM · QM (quality inspections) · FI/CO (cost postings)\n\n"
        "**Key Master Data:** Material Master · BOM (CS01) · Routing (CA01) · Work Centre (CR01)"
    ),
}


def _kb_answer_extended(query: str) -> str:
    """
    Extended KB lookup that covers v2 functional topics.
    Falls through to original _kb_answer if not found here.
    """
    t = query.lower().strip().rstrip("?")
    q_words = set(t.split())

    best_score = 0.0
    best_answer = ""

    for key, answer in _FUNCTIONAL_KB_EXTENDED.items():
        key_words = set(key.lower().split())
        if not key_words:
            continue
        matches = len(q_words & key_words)
        if matches >= max(1, len(key_words) // 2):
            score = matches / len(key_words)
            if score > best_score:
                best_score = score
                best_answer = answer

    return best_answer if best_score >= 0.5 else ""


def answer_functional_v2(query: str) -> str:
    """
    Enhanced functional answer engine (v2).
    Checks extended KB first, then falls back to original answer_from_docs.
    Also generates ABAP/CDS code if the question includes generation intent.
    """
    t = query.lower()
    topic = query.strip().rstrip("?").title()

    # If query asks to generate code alongside the explanation
    generate_code_hint = any(k in t for k in [
        "give me code", "show code", "generate code", "write code",
        "abap code", "cds view", "rap model", "example code",
        "with example", "with abap", "and abap", "and code",
    ])

    # Check extended KB
    ext = _kb_answer_extended(query)
    if ext:
        result = "📖 **" + topic + "**\n\n" + ext + "\n\n---\n*Source: SAP Functional Knowledge Base (v2)*"
        if generate_code_hint:
            code_resp = generate_abap_enhanced(query)
            result += "\n\n---\n\n" + code_resp
        return result

    # Fall back to original answer engine
    return answer_from_docs(query)


# ═══════════════════════════════════════════════════════════════════════════════
# DYNAMIC IFLOW CREATION ENGINE  (v5 — new feature)
# Three modes: (1) Pure dynamic from prompt  (2) Template-based  (3) Fallback skeleton
# ═══════════════════════════════════════════════════════════════════════════════

def is_dynamic_iflow_request(prompt: str) -> bool:
    """
    Detect requests that want a NEW iFlow built from description,
    NOT from a trained template. Triggered by:
      - Rich scenario descriptions mentioning integration, scenario, flow, connect
      - "build me", "create me", "design", "new iflow"
      - Multi-step flows: "read from ... and post to ...", "fetch ... then send"
      - Explicit: "dynamic", "custom iflow", "from scratch", "without template"
    """
    t = prompt.lower()
    dynamic_kws = [
        "build me", "create me", "design a", "design an",
        "new iflow", "new i-flow", "from scratch", "custom iflow",
        "dynamic iflow", "without template",
        "integration scenario", "integration flow",
        "connect", "integrate", "send to", "post to", "push to",
        "read from", "fetch from", "pull from",
        "trigger", "scenario", "end to end", "end-to-end",
        "workflow", "orchestration",
    ]
    # Also trigger if prompt is long and describes a business scenario
    is_long_scenario = len(prompt.split()) > 12 and any(
        k in t for k in ["when", "then", "after", "before", "upon", "trigger",
                          "system", "s4hana", "sap", "odata", "api", "service"]
    )
    return any(kw in t for kw in dynamic_kws) or is_long_scenario


def wants_template_iflow(prompt: str) -> bool:
    """Detect explicit request to use a trained template."""
    t = prompt.lower()
    return any(k in t for k in [
        "use template", "from template", "based on template",
        "trained template", "pick template", "select template",
        "smartapp template", "existing template",
    ])


def parse_dynamic_iflow_intent(prompt: str) -> Dict:
    """
    Enhanced intent parser for dynamic/descriptive iFlow requests.
    Extracts: operation, source system, target system, entity, transformations,
    error handling, groovy requirements, adapter types.
    """
    t = prompt.lower()
    cfg = parse_intent(prompt)   # start with base parse

    # ── Groovy: always include for dynamic requests ──────────────────────────
    cfg["groovy_needed"] = True  # Dynamic iFlows always include Groovy

    # ── Detect multi-step / transformation requirements ──────────────────────
    cfg["has_mapping"]    = any(k in t for k in ["map", "mapping", "transform", "convert", "enrich"])
    cfg["has_filter"]     = any(k in t for k in ["filter", "condition", "if ", "when ", "only if"])
    cfg["has_error"]      = any(k in t for k in ["error", "exception", "retry", "fallback", "fail"])
    cfg["has_split"]      = any(k in t for k in ["split", "batch", "bulk", "multiple", "loop"])
    cfg["has_enrichment"] = any(k in t for k in ["enrich", "lookup", "fetch additional", "join"])

    # ── Source / Target system detection ────────────────────────────────────
    source_map = {
        "salesforce": "Salesforce", "sfdc": "Salesforce",
        "workday": "Workday", "ariba": "SAP Ariba",
        "successfactors": "SuccessFactors", "sf ": "SuccessFactors",
        "concur": "SAP Concur", "fieldglass": "SAP Fieldglass",
        "third party": "3rd Party System", "external": "External System",
        "sftp": "SFTP Server", "ftp": "FTP Server",
        "soap": "SOAP Service", "rest": "REST API",
        "s4hana": "SAP S/4HANA", "s/4hana": "SAP S/4HANA",
        "ecc": "SAP ECC", "erp": "SAP ERP",
        "btp": "SAP BTP",
    }
    cfg["source_system"] = "Sender System"
    cfg["target_system"] = "SAP S/4HANA"
    for key, val in source_map.items():
        if key in t:
            if "from " + key in t or "source" in t:
                cfg["source_system"] = val
            elif "to " + key in t or "target" in t:
                cfg["target_system"] = val
            else:
                # Default: non-SAP = source, SAP = target
                if "s4" in key or "sap" in key or "ecc" in key:
                    cfg["target_system"] = val
                else:
                    cfg["source_system"] = val

    # ── Adapter type detection ───────────────────────────────────────────────
    if "sftp" in t:
        cfg["sender_adapter"] = "SFTP"
    elif "soap" in t and "sender" in t:
        cfg["sender_adapter"] = "SOAP"
    elif "idoc" in t:
        cfg["sender_adapter"] = "IDoc"
        cfg["receiver_adapter"] = "IDoc"

    return cfg


def build_dynamic_groovy(cfg: Dict) -> str:
    """Generate Groovy script for dynamic iFlows."""
    op     = cfg.get("operation", "GET")
    entity = cfg.get("entity_name", "Entity") or "Entity"
    path   = cfg.get("sender_path", "/api") or "/api"
    source = cfg.get("source_system", "Sender")
    target = cfg.get("target_system", "SAP S/4HANA")
    has_mapping    = cfg.get("has_mapping", False)
    has_filter     = cfg.get("has_filter", False)
    has_error      = cfg.get("has_error", False)
    has_split      = cfg.get("has_split", False)
    has_enrichment = cfg.get("has_enrichment", False)

    NL = "\n"
    lines = [
        "import com.sap.gateway.ip.core.customdev.util.Message",
        "import groovy.json.JsonSlurper",
        "import groovy.json.JsonOutput",
        "import java.text.SimpleDateFormat",
        "",
        f"// Dynamic iFlow Groovy — Entity: {entity} | Op: {op}",
        f"// Source: {source}  Target: {target}",
        f"// Path: {path}",
        "",
        "def Message processData(Message message) {",
        "    def log = messageLogFactory.getMessageLog(message)",
        "    try {",
        "        def body    = message.getBody(String.class)",
        "        def headers = message.getHeaders()",
        "        def props   = message.getProperties()",
        "",
        f"        if (log) log.addAttachmentAsString('Input_{op}', body, 'application/json')",
        "",
    ]

    if op == "GET":
        lines += [
            "        def json    = new JsonSlurper().parseText(body ?: '{}')",
            "        def records = json?.d?.results ?: (json?.value ?: [])",
            "",
        ]
        if has_filter:
            lines += [
                "        // Filter records",
                "        records = records.findAll { r -> r?.Status != 'CANCELLED' }",
                "",
            ]
        if has_mapping:
            lines += [
                "        // Map fields",
                "        def mapped = records.collect { r -> [",
                f"            id          : r?.ObjectID ?: r?.ID ?: '',",
                "            description : r?.Description ?: r?.Name ?: '',",
                "            status      : r?.Status ?: 'ACTIVE',",
                "            createdAt   : r?.CreatedAt ?: new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\").format(new Date()),",
                "        ]}",
                "        message.setBody(JsonOutput.toJson([results: mapped, count: mapped.size()]))",
            ]
        else:
            lines.append("        message.setBody(JsonOutput.toJson([results: records, count: records.size()]))")
        lines += [
            "        message.setHeader('Content-Type', 'application/json')",
            "        message.setHeader('X-Record-Count', (records?.size() ?: 0).toString())",
        ]

    elif op in ("CREATE", "UPDATE"):
        lines += [
            "        def input = new JsonSlurper().parseText(body ?: '{}')",
            "        if (!input) throw new Exception('Empty payload received')",
            "",
        ]
        if has_mapping:
            lines += [
                "        def now = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\").format(new Date())",
                "        def payload = [",
                "            ExternalID  : input?.id ?: input?.externalId ?: '',",
                "            Description : input?.description ?: input?.name ?: '',",
                "            Status      : input?.status ?: 'ACTIVE',",
                "            CreatedBy   : 'CPI_INTEGRATION',",
                "            CreatedAt   : now,",
                "        ]",
                "        if (!payload.ExternalID) throw new Exception('Required: ExternalID / id')",
                "        message.setBody(JsonOutput.toJson(payload))",
            ]
        else:
            lines.append("        message.setBody(JsonOutput.toJson(input))")
        lines.append("        message.setHeader('Content-Type', 'application/json')")
        if op == "UPDATE":
            lines.append("        message.setHeader('X-HTTP-Method', 'PATCH')")

    elif op == "DELETE":
        lines += [
            "        def input = new JsonSlurper().parseText(body ?: '{}')",
            "        def key = input?.id ?: input?.ObjectID ?: headers?.get('entityKey') ?: ''",
            "        if (!key) throw new Exception('Entity key missing for DELETE')",
            "        message.setProperty('entityKey', key)",
            "        message.setHeader('Content-Type', 'application/json')",
        ]

    if has_split:
        lines += ["", "        // TODO: split/batch logic", ""]
    if has_enrichment:
        lines += ["", "        // TODO: enrichment/lookup logic", ""]

    lines += [""]
    lines.append("    } catch (Exception e) {")
    if has_error:
        lines += [
            "        def errPayload = JsonOutput.toJson([error: e.getMessage(), context: '" + entity + "'])",
            "        message.setBody(errPayload)",
            "        message.setHeader('X-Error', 'true')",
            "        // throw new Exception('Failed: ' + e.getMessage(), e)",
        ]
    else:
        lines += [
            "        message.setBody(JsonOutput.toJson([error: e.getMessage()]))",
            "        throw new Exception('Groovy error: ' + e.getMessage(), e)",
        ]
    lines += ["    }", "    return message", "}"]

    return NL.join(lines)

def build_dynamic_iflow_skeleton(cfg: Dict) -> str:
    """
    Build a fully described iFlow XML from dynamic config.
    Adds Groovy script step and richer step names based on the scenario.
    """
    op     = cfg.get("operation", "GET")
    entity = cfg.get("entity_name", "A_Entity") or "A_Entity"
    name   = cfg.get("iflow_name", f"{op}_iFlow")
    source = cfg.get("source_system", "Sender")
    target = cfg.get("target_system", "SAP S/4HANA")

    method_map = {"GET":"GET","CREATE":"POST","UPDATE":"PUT","DELETE":"DELETE"}
    http_method = method_map.get(op, "GET")

    xml  = '<?xml version="1.0" encoding="UTF-8"?>'
    xml += '<bpmn2:definitions'
    xml += ' xmlns:bpmn2="http://www.omg.org/spec/BPMN/20100524/MODEL"'
    xml += ' xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"'
    xml += ' xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"'
    xml += ' xmlns:di="http://www.omg.org/spec/DD/20100524/DI"'
    xml += ' xmlns:ifl="http:///com.sap.ifl.model/Ifl.xsd"'
    xml += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    xml += ' id="Definitions_1">'

    xml += '<bpmn2:collaboration id="Collaboration_1" name="Default Collaboration">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>namespaceMapping</key><value/></ifl:property>'
    xml += '<ifl:property><key>httpSessionHandling</key><value>None</value></ifl:property>'
    xml += '<ifl:property><key>returnExceptionToSender</key><value>true</value></ifl:property>'
    xml += '<ifl:property><key>log</key><value>All events</value></ifl:property>'
    xml += '<ifl:property><key>componentVersion</key><value>1.2</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::IFlowVariant/cname::IFlowConfiguration/version::1.2.2</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += f'<bpmn2:participant id="Participant_1" ifl:type="EndpointSender" name="{source}">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>enableBasicAuthentication</key><value>false</value></ifl:property>'
    xml += '<ifl:property><key>ifl:type</key><value>EndpointSender</value></ifl:property>'
    xml += '</bpmn2:extensionElements></bpmn2:participant>'
    xml += f'<bpmn2:participant id="Participant_2" ifl:type="EndpointRecevier" name="{target}">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>ifl:type</key><value>EndpointRecevier</value></ifl:property>'
    xml += '</bpmn2:extensionElements></bpmn2:participant>'
    xml += '<bpmn2:participant id="Participant_Process_1" ifl:type="IntegrationProcess" name="Integration Process" processRef="Process_1">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>ifl:type</key><value>IntegrationProcess</value></ifl:property>'
    xml += '</bpmn2:extensionElements></bpmn2:participant>'
    xml += f'<bpmn2:messageFlow id="MessageFlow_1" name="{name}" sourceRef="Participant_1" targetRef="StartEvent_1"/>'
    xml += f'<bpmn2:messageFlow id="MessageFlow_2" name="{name}" sourceRef="EndEvent_1" targetRef="Participant_2"/>'
    xml += '</bpmn2:collaboration>'

    xml += '<bpmn2:process id="Process_1" name="Integration Process">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>transactionTimeout</key><value>30</value></ifl:property>'
    xml += '<ifl:property><key>componentVersion</key><value>1.1</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowElementVariant/cname::IntegrationProcess/version::1.1.3</value></ifl:property>'
    xml += '<ifl:property><key>transactionalHandling</key><value>Required</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'

    # Start Event
    xml += '<bpmn2:startEvent id="StartEvent_1" name="Receive Request">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>StartEvent</value></ifl:property>'
    xml += f'<ifl:property><key>address</key><value>%%SENDER_PATH%%</value></ifl:property>'
    xml += f'<ifl:property><key>httpMethod</key><value>{http_method}</value></ifl:property>'
    xml += '<ifl:property><key>enableBasicAuthentication</key><value>false</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowstepVariant/cname::StartEvent</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:outgoing>SF_1</bpmn2:outgoing>'
    xml += '</bpmn2:startEvent>'

    # Content Modifier
    xml += '<bpmn2:callActivity id="CA_SetHeaders" name="Set Request Headers">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>ContentModifier</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowstepVariant/cname::ContentModifier</value></ifl:property>'
    xml += f'<ifl:property><key>messageHeaderTable</key><value>Accept=application/json&#xA;CamelHttpMethod={http_method}</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SF_1</bpmn2:incoming>'
    xml += '<bpmn2:outgoing>SF_2</bpmn2:outgoing>'
    xml += '</bpmn2:callActivity>'

    # Groovy Transform
    slug = safe_slug(name)
    xml += f'<bpmn2:callActivity id="CA_Groovy" name="Transform / Map">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>ScriptTask</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowstepVariant/cname::ScriptTask</value></ifl:property>'
    xml += f'<ifl:property><key>scriptFile</key><value>script/{slug}_transform.groovy</value></ifl:property>'
    xml += '<ifl:property><key>ScriptType</key><value>Groovy</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SF_2</bpmn2:incoming>'
    xml += '<bpmn2:outgoing>SF_3</bpmn2:outgoing>'
    xml += '</bpmn2:callActivity>'

    # OData Receiver
    xml += f'<bpmn2:callActivity id="CA_OData" name="{op} {entity}">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>ODataV2Receiver</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowstepVariant/cname::ODataV2Receiver</value></ifl:property>'
    xml += '<ifl:property><key>address</key><value>%%ODATA_ADDRESS%%</value></ifl:property>'
    xml += f'<ifl:property><key>entitySetName</key><value>{entity}</value></ifl:property>'
    xml += f'<ifl:property><key>operation</key><value>{op}</value></ifl:property>'
    xml += '<ifl:property><key>authenticationMethod</key><value>BasicAuthentication</value></ifl:property>'
    xml += '<ifl:property><key>credentialName</key><value>S4HANA_CRED</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SF_3</bpmn2:incoming>'
    xml += '<bpmn2:outgoing>SF_4</bpmn2:outgoing>'
    xml += '</bpmn2:callActivity>'

    # End Event
    xml += '<bpmn2:endEvent id="EndEvent_1" name="Send Response">'
    xml += '<bpmn2:extensionElements>'
    xml += '<ifl:property><key>activityType</key><value>MessageEndEvent</value></ifl:property>'
    xml += '<ifl:property><key>cmdVariantUri</key><value>ctype::FlowstepVariant/cname::MessageEndEvent</value></ifl:property>'
    xml += '</bpmn2:extensionElements>'
    xml += '<bpmn2:incoming>SF_4</bpmn2:incoming>'
    xml += '<bpmn2:messageEventDefinition/>'
    xml += '</bpmn2:endEvent>'

    # Sequence flows
    xml += '<bpmn2:sequenceFlow id="SF_1" sourceRef="StartEvent_1" targetRef="CA_SetHeaders"/>'
    xml += '<bpmn2:sequenceFlow id="SF_2" sourceRef="CA_SetHeaders" targetRef="CA_Groovy"/>'
    xml += '<bpmn2:sequenceFlow id="SF_3" sourceRef="CA_Groovy" targetRef="CA_OData"/>'
    xml += '<bpmn2:sequenceFlow id="SF_4" sourceRef="CA_OData" targetRef="EndEvent_1"/>'
    xml += '</bpmn2:process>'

    # BPMN Diagram
    xml += '<bpmndi:BPMNDiagram id="BPMNDiagram_1" name="Default Collaboration Diagram">'
    xml += '<bpmndi:BPMNPlane bpmnElement="Collaboration_1" id="BPMNPlane_1">'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_1" id="BPMNShape_P1" isHorizontal="true"><dc:Bounds height="200.0" width="100.0" x="30.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_Process_1" id="BPMNShape_PP1" isHorizontal="true"><dc:Bounds height="200.0" width="840.0" x="130.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="Participant_2" id="BPMNShape_P2" isHorizontal="true"><dc:Bounds height="200.0" width="100.0" x="970.0" y="50.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="StartEvent_1" id="BPMNShape_SE"><dc:Bounds height="32.0" width="32.0" x="180.0" y="134.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="CA_SetHeaders" id="BPMNShape_CM"><dc:Bounds height="60.0" width="100.0" x="265.0" y="120.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="CA_Groovy" id="BPMNShape_GR"><dc:Bounds height="60.0" width="100.0" x="430.0" y="120.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="CA_OData" id="BPMNShape_OD"><dc:Bounds height="60.0" width="100.0" x="595.0" y="120.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNShape bpmnElement="EndEvent_1" id="BPMNShape_EE"><dc:Bounds height="32.0" width="32.0" x="760.0" y="134.0"/></bpmndi:BPMNShape>'
    xml += '<bpmndi:BPMNEdge bpmnElement="MessageFlow_1" id="BPMNEdge_MF1"><di:waypoint x="130.0" y="150.0"/><di:waypoint x="180.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="MessageFlow_2" id="BPMNEdge_MF2"><di:waypoint x="792.0" y="150.0"/><di:waypoint x="970.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SF_1" id="BPMNEdge_SF1"><di:waypoint x="212.0" y="150.0"/><di:waypoint x="265.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SF_2" id="BPMNEdge_SF2"><di:waypoint x="365.0" y="150.0"/><di:waypoint x="430.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SF_3" id="BPMNEdge_SF3"><di:waypoint x="530.0" y="150.0"/><di:waypoint x="595.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '<bpmndi:BPMNEdge bpmnElement="SF_4" id="BPMNEdge_SF4"><di:waypoint x="695.0" y="150.0"/><di:waypoint x="760.0" y="150.0"/></bpmndi:BPMNEdge>'
    xml += '</bpmndi:BPMNPlane></bpmndi:BPMNDiagram>'
    xml += '</bpmn2:definitions>'
    return xml


def generate_flow_diagram_text(cfg: Dict) -> str:
    """Generate ASCII flow diagram for the iFlow design."""
    op     = cfg.get("operation", "GET")
    entity = cfg.get("entity_name", "A_Entity") or "A_Entity"
    path   = cfg.get("sender_path", "/api/v1") or "/api/v1"
    source = cfg.get("source_system", "Sender System")
    target = cfg.get("target_system", "SAP S/4HANA")
    name   = cfg.get("iflow_name", f"{op}_iFlow")
    url    = cfg.get("odata_address", "") or "[Your S/4HANA URL]"
    method_map = {"GET":"GET","CREATE":"POST","UPDATE":"PUT","DELETE":"DELETE"}
    http_method = method_map.get(op, "GET")

    steps = [
        ("START",  f"Receive {http_method} from {source}",       f"Path: {path}"),
        ("STEP 1", "Content Modifier — Set Headers",              f"Accept: application/json | {http_method}"),
        ("STEP 2", "Groovy Script — Transform / Map",             f"Payload transform for {entity}"),
        ("STEP 3", f"OData Receiver — {op} {entity}",            f"URL: {url} | Auth: BasicAuthentication"),
        ("END",    f"Return response to {source}",                "HTTP response"),
    ]
    if cfg.get("has_filter"):
        steps.insert(2, ("FILTER", "Router / Filter", "Condition-based routing"))
    if cfg.get("has_split"):
        steps.insert(2, ("SPLIT",  "Splitter",         "Batch/bulk split"))
    if cfg.get("has_enrichment"):
        steps.insert(3, ("ENRICH", "Content Enricher", "Secondary lookup"))

    header = (
        "```\n"
        "╔══════════════════════════════════════════════════════════╗\n"
        f"║  iFlow : {name:<50}║\n"
        f"║  {source} → {target:<43}║\n"
        "╚══════════════════════════════════════════════════════════╝\n"
    )
    body_lines = []
    for i, (icon, title, detail) in enumerate(steps):
        body_lines.append(f"  [{icon}]  {title}")
        body_lines.append(f"          └─ {detail}")
        if i < len(steps) - 1:
            body_lines.append("               │")
            body_lines.append("               ▼")
    footer = "\n```"

    notes = [
        "\n**Configuration Notes:**",
        f"- **Sender Adapter:** `{cfg.get('sender_adapter','HTTPS')}`",
        f"- **Receiver Adapter:** `{cfg.get('receiver_adapter','OData V2')}`",
        f"- **HTTP Method:** `{http_method}`",
        f"- **Entity:** `{entity}`",
        f"- **Endpoint:** `{path}`",
    ]

    return header + "\n".join(body_lines) + footer + "\n".join(notes)

def generate_template_picker_message(index: List[Dict], op: str) -> str:
    """Show available trained templates when iFlow fails to work."""
    matching = [r for r in index if r.get("operation","").upper() == op.upper()]
    if not matching:
        return (
            f"No trained templates found for **{op}** operations.\n\n"
            "Go to **Upload iFlows** to upload a working ZIP, then **Train Index** to register it."
        )
    lines = [
        f"## Available Trained Templates — {op}\n",
        "Use the **Template Preference** filter in the sidebar to select one:\n",
    ]
    for i, rec in enumerate(matching[:12], 1):
        p    = rec.get("props", {})
        ent  = p.get("entity_name", "—")
        path = p.get("sender_path", "—")
        lines.append(f"**{i}. `{rec['name']}`** | Entity: `{ent}` | Path: `{path}`")
    lines += [
        "\n---",
        "**To use a template:** Copy its name → paste into 🎯 Template Preference → ask again.",
    ]
    return "\n".join(lines)

def _build_dynamic_summary(cfg: Dict, artifact_id: str,
                             source: str, groovy_code: str,
                             zip_bytes: bytes) -> str:
    op     = cfg.get("operation","GET")
    entity = cfg.get("entity_name","—") or "—"
    path   = cfg.get("sender_path","—") or "—"
    url    = cfg.get("odata_address","") or "(not set)"
    src_s  = cfg.get("source_system","Sender")
    tgt_s  = cfg.get("target_system","SAP S/4HANA")
    return f"""✅ **Dynamic iFlow generated**

| Field | Value |
|---|---|
| **iFlow Name** | `{artifact_id}` |
| **Operation** | `{op}` |
| **Sender Path** | `{path}` |
| **Entity** | `{entity}` |
| **OData URL** | `{url}` |
| **Source → Target** | {src_s} → {tgt_s} |
| **Source** | {source} |
| **Groovy** | ✓ Custom script ({op} + transform + error handling) |
| **ZIP Size** | {round(len(zip_bytes)/1024,1)} KB |"""



def generate_dynamic_iflow(prompt: str, index: List[Dict],
                            preferred_template: str = "") -> tuple:
    """
    Main entry point for dynamic iFlow generation.
    Returns: (zip_bytes, groovy_code, summary, flow_diagram)
    """
    cfg = parse_dynamic_iflow_intent(prompt)
    op  = cfg["operation"]

    entity_short = (cfg.get("entity_name") or "").replace("A_","") or "Entity"
    cfg["iflow_name"] = safe_slug(
        f"{op.title()}_{entity_short}_iFlow"
        if entity_short != "Entity"
        else f"{op.title()}_Custom_iFlow"
    )

    artifact_id = cfg["iflow_name"]
    sender_path = cfg["sender_path"] or f"/{entity_short}/{op.title()}"
    entity_name = cfg.get("entity_name") or "A_Entity"
    odata_addr  = cfg.get("odata_address", "")
    cfg["sender_path"] = sender_path

    groovy_code = build_dynamic_groovy(cfg)

    # Try preferred template first
    if preferred_template and index:
        matched = find_best_match(index, op, entity_name, sender_path,
                                   cfg.get("sender_adapter","HTTPS"),
                                   preferred_template=preferred_template)
        if matched:
            original = load_template_zip(matched["id"])
            if original:
                try:
                    p = matched.get("props", {})
                    subs = [
                        (matched.get("name",""), artifact_id),
                        (matched.get("id",""), artifact_id),
                        (p.get("sender_path",""), sender_path),
                        (p.get("entity_name",""), entity_name),
                        (p.get("odata_address",""), odata_addr),
                    ]
                    zip_bytes = clone_and_patch_zip(original, subs, artifact_id,
                                                     artifact_id, groovy_code)
                    tmpl_src = f"Cloned from: `{matched['name']}`"
                    flow_diagram = generate_flow_diagram_text(cfg)
                    summary = _build_dynamic_summary(cfg, artifact_id, tmpl_src,
                                                      groovy_code, zip_bytes)
                    return zip_bytes, groovy_code, summary, flow_diagram
                except Exception:
                    pass

    # Build from dynamic skeleton
    iflow_xml = build_dynamic_iflow_skeleton(cfg)
    for ph, val in [
        ("%%IFLOW_NAME%%", artifact_id),
        ("%%SENDER_PATH%%", sender_path),
        ("%%ENTITY_NAME%%", entity_name),
        ("%%ODATA_ADDRESS%%", odata_addr),
    ]:
        iflow_xml = iflow_xml.replace(ph, val)

    zip_bytes    = build_zip_from_skeleton(artifact_id, artifact_id, iflow_xml, groovy_code)
    flow_diagram = generate_flow_diagram_text(cfg)
    summary      = _build_dynamic_summary(cfg, artifact_id,
                                           "Built from dynamic skeleton",
                                           groovy_code, zip_bytes)
    return zip_bytes, groovy_code, summary, flow_diagram



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

def generate_iflow(cfg: Dict, index: List[Dict],
                   preferred_template: str = "") -> Tuple[bytes, str, str]:
    op          = cfg["operation"]
    # artifact_id = underscore slug  (CPI ID field — no spaces allowed)
    # iflow_name  = same slug        (used internally; make_manifest converts to display name)
    raw_name    = cfg["iflow_name"] or f"{op}_iFlow"
    artifact_id = safe_slug(raw_name)   # e.g. Get_PurchaseOrder_iFlow
    iflow_name  = artifact_id           # keep consistent; make_manifest adds spaces for display
    sender_path = cfg["sender_path"] or f"/{safe_slug(raw_name.lower())}"
    entity_name = cfg["entity_name"] or "A_Entity"
    odata_addr  = cfg.get("odata_address","")

    matched    = find_best_match(index, op, entity_name, sender_path,
                                  cfg.get("sender_adapter","HTTPS"),
                                  preferred_template=preferred_template)
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
            try:
                zip_bytes = clone_and_patch_zip(original, subs, iflow_name,
                                                 artifact_id, groovy_code)
            except ValueError as _ve:
                # Template is a package-wrapper ZIP (no .iflw) — fall back to skeleton
                tmpl_src  = f"built-in skeleton (template '{tmpl_name}' is a package export, not a single iFlow)"
                tmpl_name = f"Built-in {op} skeleton"
                xml = SKELETONS.get(op, SKELETONS["GET"])
                for ph, val in [("%%IFLOW_NAME%%",iflow_name),("%%SENDER_PATH%%",sender_path),
                                ("%%ENTITY_NAME%%",entity_name),("%%ODATA_ADDRESS%%",odata_addr)]:
                    xml = xml.replace(ph, val)
                zip_bytes = build_zip_from_skeleton(iflow_name, artifact_id, xml, groovy_code)
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

    pref_row = (f"| **Template Filter** | `{preferred_template}` |\n"
                if preferred_template else "")
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
{pref_row}| **Groovy** | {"✓ Included (" + op + " pattern)" if groovy_code else "Not included"} |
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
                # Write MANIFEST.MF first and UNCOMPRESSED if present
                names = zin.namelist()
                mf_name = next((n for n in names if n.upper().endswith("MANIFEST.MF")), None)
                if mf_name:
                    mf_raw = zin.read(mf_name)
                    new_mf, mf_changes = _replace_bytes_recursive(mf_raw, replacements)
                    modified_count += mf_changes
                    mf_info = zipfile.ZipInfo(mf_name)
                    mf_info.compress_type = zipfile.ZIP_STORED
                    zout.writestr(mf_info, new_mf)

                for item in zin.infolist():
                    if item.filename == mf_name:
                        continue   # already written above
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
    page_title="SAP Intelligence Suite — CPI + ABAP + Functional",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* ═══════════════════════════════════════════════════════
   SAP Intelligence Suite v2 — Full Dark Theme
   All text: bright white (#e6edf3) on dark backgrounds
   Every Streamlit element forced into dark mode
   ═══════════════════════════════════════════════════════ */

/* ── Root app background & default text ─────────────────────────────────── */
html, body, [class*="css"], .stApp {
    background-color:#0d1117 !important;
    color:#e6edf3 !important;
    font-family:'Segoe UI', system-ui, sans-serif !important;
}

/* ── Force ALL text elements bright white ────────────────────────────────── */
p, span, div, h1, h2, h3, h4, h5, h6, li, td, th, label, small,
a, strong, em, code, pre, blockquote {
    color:#e6edf3 !important;
}

/* ── Streamlit markdown / text components ────────────────────────────────── */
.stMarkdown, .stMarkdown p, .stMarkdown li, .stMarkdown span,
.stText, [data-testid="stMarkdownContainer"],
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span,
[data-testid="stMarkdownContainer"] strong,
[data-testid="stMarkdownContainer"] em,
[data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3 {
    color:#e6edf3 !important;
}

/* ── Caption / helper text — slightly muted ──────────────────────────────── */
.stCaption, [data-testid="stCaptionContainer"],
[data-testid="stCaptionContainer"] * {
    color:#8b949e !important;
}

/* ── Headers (st.title, st.header etc.) ──────────────────────────────────── */
[data-testid="stHeading"] h1, [data-testid="stHeading"] h2,
[data-testid="stHeading"] h3, .css-10trblm,
h1.stTitle, h2, h3 {
    color:#e6edf3 !important;
}

/* ── Sidebar ─────────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background:#161b22 !important;
    border-right:1px solid #30363d !important;
}
section[data-testid="stSidebar"] *,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color:#e6edf3 !important;
}
section[data-testid="stSidebar"] small,
section[data-testid="stSidebar"] .stCaption,
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] * {
    color:#8b949e !important;
}

/* ── Sidebar radio (mode selector) ──────────────────────────────────────── */
section[data-testid="stSidebar"] .stRadio > div,
section[data-testid="stSidebar"] .stRadio label,
section[data-testid="stSidebar"] .stRadio span,
section[data-testid="stSidebar"] .stRadio p {
    color:#e6edf3 !important;
    font-size:13px !important;
}

/* ── ALL buttons (sidebar + main) ────────────────────────────────────────── */
.stButton > button,
section[data-testid="stSidebar"] .stButton > button {
    background:#21262d !important;
    color:#ffffff !important;
    border:1px solid #388bfd !important;
    border-radius:8px !important;
    font-weight:600 !important;
    font-size:13px !important;
    transition:background 0.15s, border-color 0.15s !important;
    padding:8px 14px !important;
}
.stButton > button:hover,
section[data-testid="stSidebar"] .stButton > button:hover {
    background:#1f6feb !important;
    border-color:#58a6ff !important;
    color:#ffffff !important;
}
.stButton > button p,
section[data-testid="stSidebar"] .stButton > button p,
.stButton > button span,
section[data-testid="stSidebar"] .stButton > button span {
    color:#ffffff !important;
    font-weight:600 !important;
}
/* Primary button */
.stButton > button[kind="primary"] {
    background:#1f6feb !important;
    border-color:#388bfd !important;
}

/* ── Expanders (sidebar + main) ──────────────────────────────────────────── */
div[data-testid="stExpander"],
section[data-testid="stSidebar"] div[data-testid="stExpander"] {
    background:#161b22 !important;
    border:1px solid #30363d !important;
    border-radius:8px !important;
}
div[data-testid="stExpander"] summary,
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary {
    background:#21262d !important;
    border-radius:8px !important;
}
div[data-testid="stExpander"] summary span,
div[data-testid="stExpander"] summary p,
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary span,
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary p {
    color:#e6edf3 !important;
    font-weight:600 !important;
}

/* ── All text inputs / textareas ─────────────────────────────────────────── */
input, textarea,
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
    background:#21262d !important;
    color:#e6edf3 !important;
    border:1px solid #30363d !important;
    border-radius:8px !important;
    font-size:13px !important;
}
input::placeholder, textarea::placeholder { color:#8b949e !important; }
input:focus, textarea:focus {
    border-color:#388bfd !important;
    box-shadow:0 0 0 2px rgba(31,111,235,0.3) !important;
    outline:none !important;
}

/* ── Select / dropdown ───────────────────────────────────────────────────── */
select, .stSelectbox > div > div {
    background:#21262d !important;
    color:#e6edf3 !important;
    border:1px solid #30363d !important;
    border-radius:8px !important;
}

/* ── Checkboxes & radio visible labels ───────────────────────────────────── */
.stCheckbox label, .stCheckbox span,
.stRadio label,   .stRadio span {
    color:#e6edf3 !important;
}

/* ── File uploader ───────────────────────────────────────────────────────── */
[data-testid="stFileUploader"],
[data-testid="stFileUploader"] section,
[data-testid="stFileUploadDropzone"] {
    background:#161b22 !important;
    border:1px dashed #30363d !important;
    border-radius:8px !important;
    color:#e6edf3 !important;
}
[data-testid="stFileUploader"] span,
[data-testid="stFileUploader"] p,
[data-testid="stFileUploader"] small,
[data-testid="stFileUploadDropzone"] span,
[data-testid="stFileUploadDropzone"] p {
    color:#e6edf3 !important;
}

/* ── Tables / DataFrames ─────────────────────────────────────────────────── */
.stDataFrame, .stTable,
[data-testid="stTable"], [data-testid="stDataFrame"] {
    background:#161b22 !important;
    color:#e6edf3 !important;
}
.stDataFrame th, .stDataFrame td,
[data-testid="stTable"] th, [data-testid="stTable"] td {
    background:#161b22 !important;
    color:#e6edf3 !important;
    border-color:#30363d !important;
}
.stDataFrame thead th {
    background:#21262d !important;
    color:#58a6ff !important;
    font-weight:700 !important;
}

/* ── Info / Warning / Success / Error boxes ──────────────────────────────── */
[data-testid="stAlert"],
div.stAlert { border-radius:8px !important; }
[data-testid="stAlert"] p,
[data-testid="stAlert"] span,
div.stAlert p { color:#e6edf3 !important; }
[data-testid="stNotification"] { border-radius:8px !important; }

/* ── st.info ─────────────────────────────────────────────────────────────── */
div[data-baseweb="notification"][kind="info"],
.stAlert[data-baseweb="notification"] {
    background:#1f3a5f !important;
    border-left:3px solid #388bfd !important;
    color:#e6edf3 !important;
}
/* ── st.success ──────────────────────────────────────────────────────────── */
div[data-baseweb="notification"][kind="positive"] {
    background:#1a3b1a !important;
    border-left:3px solid #238636 !important;
}
/* ── st.warning ──────────────────────────────────────────────────────────── */
div[data-baseweb="notification"][kind="warning"] {
    background:#3b2a1a !important;
    border-left:3px solid #9e6a03 !important;
}
/* ── st.error ────────────────────────────────────────────────────────────── */
div[data-baseweb="notification"][kind="negative"] {
    background:#3b1a1a !important;
    border-left:3px solid #da3633 !important;
}

/* ── Progress bars ───────────────────────────────────────────────────────── */
.stProgress > div > div { background:#238636 !important; }
.stProgress > div { background:#21262d !important; }

/* ── Metrics ─────────────────────────────────────────────────────────────── */
div[data-testid="metric-container"] {
    background:#161b22 !important;
    border:1px solid #30363d !important;
    border-radius:8px !important;
    padding:12px !important;
}
div[data-testid="metric-container"] label,
div[data-testid="metric-container"] [data-testid="stMetricLabel"],
div[data-testid="metric-container"] [data-testid="stMetricLabel"] * {
    color:#8b949e !important;
}
div[data-testid="stMetricValue"],
div[data-testid="stMetricValue"] * {
    color:#58a6ff !important;
    font-weight:700 !important;
}

/* ── Tabs ────────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] { background:#161b22 !important; }
.stTabs [data-baseweb="tab"] { color:#8b949e !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color:#e6edf3 !important;
    border-bottom:2px solid #388bfd !important;
}

/* ── Spinner text ────────────────────────────────────────────────────────── */
.stSpinner > div, .stSpinner p { color:#e6edf3 !important; }

/* ── Download button ─────────────────────────────────────────────────────── */
.stDownloadButton > button {
    background:#238636 !important;
    color:#ffffff !important;
    border:1px solid #2ea043 !important;
    border-radius:8px !important;
    font-weight:600 !important;
}
.stDownloadButton > button:hover {
    background:#2ea043 !important;
    color:#ffffff !important;
}
.stDownloadButton > button p { color:#ffffff !important; }

/* ── Code blocks ─────────────────────────────────────────────────────────── */
code, pre, .stCodeBlock,
[data-testid="stCodeBlock"] pre {
    background:#161b22 !important;
    color:#79c0ff !important;
    border:1px solid #30363d !important;
    border-radius:6px !important;
}

/* ── Horizontal rule ─────────────────────────────────────────────────────── */
hr { border-color:#30363d !important; }

/* ── Chat bubbles ────────────────────────────────────────────────────────── */
.msg-user { display:flex; justify-content:flex-end; margin:12px 0; }
.msg-user .bubble {
    background:#1f6feb; color:#fff;
    border-radius:18px 18px 4px 18px;
    padding:12px 18px; max-width:75%; font-size:14px; line-height:1.5;
}
.msg-bot { display:flex; justify-content:flex-start; margin:12px 0; gap:10px; }
.msg-bot .avatar {
    width:32px; height:32px;
    background:linear-gradient(135deg,#238636,#1f6feb);
    border-radius:50%; display:flex; align-items:center;
    justify-content:center; font-size:16px; flex-shrink:0;
}
.msg-bot .bubble {
    background:#161b22; border:1px solid #30363d;
    border-radius:4px 18px 18px 18px;
    padding:14px 18px; max-width:85%;
    font-size:14px; line-height:1.6; color:#e6edf3 !important;
}

/* ── Operation chips ─────────────────────────────────────────────────────── */
.chip { display:inline-block; padding:3px 10px; border-radius:12px;
    font-size:11px; font-weight:700; margin:2px; }
.chip-get    { background:#1a3b1a; color:#3fb950 !important; border:1px solid #238636; }
.chip-create { background:#1f3a5f; color:#58a6ff !important; border:1px solid #388bfd; }
.chip-update { background:#3b2a1a; color:#d29922 !important; border:1px solid #9e6a03; }
.chip-delete { background:#3b1a1a; color:#f85149 !important; border:1px solid #da3633; }

/* ── Status boxes ────────────────────────────────────────────────────────── */
.sbox { border-radius:8px; padding:12px 16px; margin:6px 0; font-size:13px; }
.sbox-ok   { background:#1a3b1a; border:1px solid #238636; color:#3fb950 !important; }
.sbox-warn { background:#3b2a1a; border:1px solid #9e6a03; color:#d29922 !important; }
.sbox-info { background:#1f3a5f; border:1px solid #388bfd; color:#58a6ff !important; }

/* ── Hide Streamlit chrome ───────────────────────────────────────────────── */
#MainMenu { visibility:hidden; }
footer    { visibility:hidden; }
header    { visibility:hidden; }
</style>
""", unsafe_allow_html=True)

# ─── Session state ─────────────────────────────────────────────────────────────
for k, v in [("messages",[]),("last_zip",None),("last_fname",None),
              ("pending_cfg",None),
              ("preferred_template",""),   # user-pinned template name filter
              ("iflow_mode","auto"),       # auto | dynamic | template | skeleton
              ("show_diagram", True),      # show flow diagram after generation
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
    st.markdown("## ⚡ SAP Intelligence Suite v2")
    st.markdown("---")
    mode = st.radio("Mode", [
        "💬 Chat & Generate",
        "📁 Upload iFlows",
        "🧠 Train Index",
        "📚 Train Docs",
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

    # ── iFlow Generation Mode ────────────────────────────────────────────
    with st.expander("⚡ iFlow Generation Mode", expanded=True):
        st.caption("Choose how iFlows are created when you type a request.")
        iflow_mode = st.radio(
            "Mode",
            options=["auto", "dynamic", "template", "skeleton"],
            format_func=lambda x: {
                "auto":     "🤖 Auto — Smart detect from prompt",
                "dynamic":  "✨ Dynamic — Always build from description",
                "template": "📋 Template — Always use trained template",
                "skeleton": "🦴 Skeleton — Always use built-in skeleton",
            }[x],
            key="iflow_mode",
            label_visibility="collapsed",
        )
        st.caption({
            "auto":     "Detects if prompt describes a scenario → dynamic; otherwise → template or skeleton.",
            "dynamic":  "Builds a fresh iFlow from your description with custom Groovy. No template needed.",
            "template": "Clones your best matching trained template. Requires trained index.",
            "skeleton": "Uses built-in GET/POST/PUT/DELETE skeleton XML. Fastest, simplest.",
        }.get(iflow_mode, ""))

        st.checkbox("Show flow diagram in response", key="show_diagram", value=True)

    # ── Template filter (only relevant in auto/template mode) ─────────────
    with st.expander("🎯 Template Preference", expanded=False):
        st.caption("Pin a template name to prefer it. Only used in Auto/Template mode.")
        st.text_input(
            "Prefer templates containing:",
            key="preferred_template",
            placeholder="e.g. Smartapp, PurchaseOrder, Journal...",
        )
        pref = st.session_state.get("preferred_template", "").strip()
        if pref:
            idx_preview = load_index()
            matches = [r for r in idx_preview
                       if pref.lower() in r.get("name","").lower()]
            if matches:
                st.success(f"✓ {len(matches)} template(s) match '{pref}'", icon="🎯")
            else:
                st.warning(f"No match for '{pref}' — will use skeleton", icon="⚠️")
        else:
            st.info("Auto-select (best match by operation + entity)", icon="ℹ️")

        # Show available templates
        idx_all = load_index()
        if idx_all:
            if st.button("📋 Show all trained templates", key="btn_show_templates",
                         use_container_width=True):
                st.session_state.pending_cfg = {"__show_templates__": True}
                st.rerun()
    st.markdown("---")

    # ── Quick prompts grouped by category ───────────────────────────────
    # Keys use enumerate index to guarantee uniqueness across all buttons
    with st.expander("⚡ iFlow Quick Prompts", expanded=False):
        iflow_quick = [
            "GET iFlow for Purchase Orders from S/4HANA with Groovy",
            "GET iFlow for Sales Orders",
            "POST iFlow to create Purchase Order with Groovy",
            "PUT iFlow to update Project Elements",
            "DELETE iFlow for A_PurchaseOrder",
            "GET iFlow for A_JournalEntryItemBasic",
            "Generate New Smartapp ONE Package",
        ]
        for i, q in enumerate(iflow_quick):
            if st.button(q, key=f"qif_{i}", use_container_width=True):
                st.session_state.messages.append({"role":"user","content":q})
                if smartapp_prompt_requested(q):
                    st.session_state.pending_cfg = {
                        "smartapp_package": True,
                        "prompt": q,
                        "old_host": st.session_state.get("smartapp_old_host", "").strip(),
                        "new_host": st.session_state.get("smartapp_new_host", "").strip(),
                        "old_cred": st.session_state.get("smartapp_old_cred", "").strip(),
                        "new_cred": st.session_state.get("smartapp_new_cred", "").strip(),
                    }
                else:
                    st.session_state.pending_cfg = parse_intent(q)
                st.rerun()

    # ── NEW: Dynamic iFlow Quick Prompts ──────────────────────────────────
    with st.expander("✨ Dynamic iFlow Quick Prompts", expanded=False):
        st.caption("Scenario-based prompts — set mode to Dynamic or Auto for best results")
        dyn_quick = [
            # Cross-system integration
            "Build an iFlow to read Purchase Orders from S/4HANA and send to Salesforce with field mapping",
            "Create integration flow to sync employee data from Workday to SAP S/4HANA",
            "Design iFlow to fetch supplier invoices from SAP Ariba and post to S/4HANA with error handling",
            "Build iFlow that reads Sales Orders from S/4HANA and pushes to a third-party logistics system",
            "Create iFlow to sync Business Partners from S/4HANA to Salesforce CRM with mapping",
            # Financial
            "Build iFlow to fetch financial commitments pooling from PO and Budget in S/4HANA",
            "Create iFlow to post journal entries from external system to SAP S/4HANA FI",
            "Design iFlow to extract GL account postings from S/4HANA and send to reporting system",
            # Procurement
            "Build iFlow to create Purchase Requisitions in S/4HANA from incoming REST requests with validation",
            "Create iFlow to sync vendor master data from SAP Ariba to S/4HANA with duplicate check",
            # HR / Projects
            "Design iFlow to replicate Project Elements from S/4HANA to external project management tool",
            "Build iFlow to sync cost centre data between S/4HANA and SuccessFactors",
            # Complex scenarios
            "Create iFlow that receives bulk material data, splits into batches, and upserts in S/4HANA",
            "Build iFlow to read delivery status from 3PL system and update S/4HANA outbound delivery",
            "Design iFlow to fetch IDOC messages from SAP ECC and convert to OData calls in S/4HANA",
        ]
        for i, q in enumerate(dyn_quick):
            if st.button(q, key=f"qdyn_{i}", use_container_width=True):
                st.session_state.messages.append({"role":"user","content":q})
                # Dynamic prompts always use dynamic mode via __prompt__ key
                cfg = parse_intent(q)
                cfg["__prompt__"] = q
                st.session_state.pending_cfg = cfg
                st.rerun()

    with st.expander("📝 ABAP & Code Quick Prompts", expanded=False):
        abap_quick = [
            "ABAP program to fetch financial commitment data pooling from PO and Budget",
            "Generate CDS View for Purchase Orders",
            "RAP model for Sales Orders with OData V4",
            "ABAP class for Purchase Order with ALV display",
            "Give me ABAP for General Ledger entries",
            "Generate CDS View for Material Master",
            "Write ABAP report for Vendor Master data",
        ]
        for i, q in enumerate(abap_quick):
            if st.button(q, key=f"qab_{i}", use_container_width=True):
                st.session_state.messages.append({"role":"user","content":q})
                st.session_state.pending_cfg = {"__abap_direct__": True, "prompt": q}
                st.rerun()

    with st.expander("🎓 Functional Knowledge Quick Prompts", expanded=False):
        func_quick = [
            "Explain Process Order in SAP",
            "What is Financial Commitment in SAP FM?",
            "Explain Order to Cash end-to-end process",
            "What is the Procure to Pay process in SAP?",
            "Explain MRP in SAP PP",
            "What is RAP in ABAP?",
            "What is a CDS View?",
            "Explain Batch Management in SAP",
            "What is Asset Accounting in SAP?",
            "Explain SAP Activate Methodology",
        ]
        for i, q in enumerate(func_quick):
            if st.button(q, key=f"qfn_{i}", use_container_width=True):
                st.session_state.messages.append({"role":"user","content":q})
                st.session_state.pending_cfg = {"__functional_direct__": True, "prompt": q}
                st.rerun()

    # ── Smartapp ONE inputs ABOVE quick-prompts so values are in session_state
    #    before any button click is processed on the same or next rerun.
    with st.expander("⚙️ Smartapp ONE Package Inputs", expanded=True):
        st.caption(
            "Fill these BEFORE clicking 'Generate New Smartapp ONE Package'. "
            "Values are saved automatically as you type."
        )
        st.text_input(
            "Current Hostname (to replace)",
            key="smartapp_old_host",
            placeholder="e.g. my401471.s4hana.cloud.sap:443",
        )
        st.text_input(
            "New Hostname (replacement)",
            key="smartapp_new_host",
            placeholder="e.g. my401478.s4hana.cloud.sap:443",
        )
        st.text_input(
            "Current Credential (to replace)",
            key="smartapp_old_cred",
            placeholder="e.g. SAPCloud",
        )
        st.text_input(
            "New Credential (replacement)",
            key="smartapp_new_cred",
            placeholder="e.g. S4HANACred",
        )

        # Show live status so user knows values are captured
        nh = st.session_state.get("smartapp_new_host", "").strip()
        nc = st.session_state.get("smartapp_new_cred", "").strip()
        if nh and nc:
            st.success(f"✓ Ready: {nh[:30]}… / {nc[:20]}…", icon="✅")
        else:
            missing = []
            if not nh: missing.append("New Hostname")
            if not nc: missing.append("New Credential")
            st.warning(f"Still needed: **{', '.join(missing)}**", icon="⚠️")

    st.markdown("---")
    if st.button("🗑 Clear chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — CHAT
# ═══════════════════════════════════════════════════════════════════════════════

if mode == "💬 Chat & Generate":

    st.markdown("### ⚡ SAP Intelligence Suite — CPI · ABAP · Functional")
    st.caption("Generate iFlows · ABAP Programs · CDS Views · RAP Models · Get SAP functional answers — all in plain English.")

    # Render history
    for msg_idx, msg in enumerate(st.session_state.messages):
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
                    key       = f"dl_{msg_idx}_{msg['zip_key']}",
                )

    # Process pending (from quick buttons)
    if st.session_state.pending_cfg is not None:
        cfg = st.session_state.pending_cfg
        st.session_state.pending_cfg = None
        zkey  = f"zip_{int(time.time()*1000)}"

        # ── Show trained templates list ───────────────────────────────────
        if cfg.get("__show_templates__"):
            index_t = load_index()
            if index_t:
                from collections import defaultdict as _dd
                by_op = _dd(list)
                for r in index_t:
                    by_op[r.get("operation","?")].append(r)
                parts = ["## All Trained Templates\n"]
                for op_name, recs in sorted(by_op.items()):
                    parts.append(f"### {op_name} ({len(recs)} templates)")
                    for r in recs[:10]:
                        p = r.get("props",{})
                        n = r["name"]; e = p.get("entity_name","--"); pt = p.get("sender_path","--")
                        parts.append(f"- **`{n}`** | Entity: `{e}` | Path: `{pt}`")
                    parts.append("")
                parts.append("---")
                parts.append("Set the Template Preference filter in the sidebar to use one.")
                reply_txt = "\n".join(parts)
                st.session_state.messages.append({"role":"assistant","content": reply_txt})
            else:
                st.session_state.messages.append({"role":"assistant","content":"No trained templates yet. Upload iFlows and train the index first."})
            st.rerun()

        # ── v2: ABAP direct quick prompt ──────────────────────────────────
        if cfg.get("__abap_direct__"):
            prompt_text = cfg.get("prompt", "")
            with st.spinner("Generating ABAP response…"):
                abap_reply = _web_answer_abap(prompt_text)
            st.session_state.messages.append({"role":"assistant","content":abap_reply})
            st.rerun()

        # ── v2: Functional direct quick prompt ────────────────────────────
        elif cfg.get("__functional_direct__"):
            prompt_text = cfg.get("prompt", "")
            with st.spinner("Finding the best answer…"):
                func_reply = answer_functional_v2(prompt_text)
            st.session_state.messages.append({"role":"assistant","content":func_reply})
            st.rerun()

        elif cfg.get("smartapp_package"):
            # Read values captured at click time (cfg) with session_state fallback
            replacements_override = {
                "old_host": cfg.get("old_host") or st.session_state.get("smartapp_old_host", "").strip(),
                "new_host": cfg.get("new_host") or st.session_state.get("smartapp_new_host", "").strip(),
                "old_cred": cfg.get("old_cred") or st.session_state.get("smartapp_old_cred", "").strip(),
                "new_cred": cfg.get("new_cred") or st.session_state.get("smartapp_new_cred", "").strip(),
            }
            missing = []
            if not replacements_override["new_host"]:
                missing.append("New Hostname")
            if not replacements_override["new_cred"]:
                missing.append("New Credential")
            if missing:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": (
                        f"⚠️ Please fill in **{', '.join(missing)}** in the "
                        "**⚙️ Smartapp ONE Package Inputs** section in the sidebar, "
                        "then click **Generate New Smartapp ONE Package** again."
                    )
                })
                st.rerun()
                st.stop()
            with st.spinner("Generating Smartapp ONE package…"):
                try:
                    zip_bytes, summary = generate_smartapp_package(
                        cfg.get("prompt", "Generate New Smartapp ONE Package"),
                        replacements_override=replacements_override,
                    )
                except FileNotFoundError as e:
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": (
                            f"⚠️ **Smartapp ONE package not found.**\n\n"
                            f"{e}\n\n"
                            "Please upload the Smartapp ONE ZIP via **📁 Upload iFlows** "
                            "and train it via **🧠 Train Index** first."
                        )
                    })
                    st.rerun()
                    st.stop()
            fname = "Smartapp_ONE_modified.zip"
            reply = summary
        else:
            index  = load_index()
            pref   = st.session_state.get("preferred_template","").strip()
            mode_s = st.session_state.get("iflow_mode","auto")
            show_d = st.session_state.get("show_diagram", True)
            prompt_text = cfg.get("__prompt__","")

            # Decide generation mode
            # If __prompt__ is set (from Dynamic quick prompt button) → always dynamic
            use_dynamic = (
                bool(prompt_text) or          # dynamic quick-prompt button sets __prompt__
                mode_s == "dynamic" or
                (mode_s == "auto" and prompt_text and is_dynamic_iflow_request(prompt_text))
            )

            if use_dynamic and prompt_text:
                with st.spinner("Building dynamic iFlow from description..."):
                    zip_bytes, groovy_code, summary, flow_diagram = generate_dynamic_iflow(
                        prompt_text, index, preferred_template=pref)
                fname = f"{safe_slug(cfg.get('iflow_name','dynamic_iflow'))}.zip"
                reply = summary
                if show_d:
                    reply += "\n\n---\n\n## Flow Diagram\n\n" + flow_diagram
                if groovy_code:
                    gc_preview = groovy_code[:1800] + ("..." if len(groovy_code)>1800 else "")
                    reply += "\n\n---\n\n**Groovy Script:**\n\n```groovy\n" + gc_preview + "\n```"
            elif mode_s == "skeleton":
                xml = SKELETONS.get(cfg["operation"], SKELETONS["GET"])
                for ph, val in [("%%IFLOW_NAME%%",cfg["iflow_name"]),
                                 ("%%SENDER_PATH%%",cfg["sender_path"]),
                                 ("%%ENTITY_NAME%%",cfg["entity_name"]),
                                 ("%%ODATA_ADDRESS%%",cfg.get("odata_address",""))]:
                    xml = xml.replace(ph, val)
                gcode = get_groovy(cfg["operation"], cfg["entity_name"], cfg["sender_path"]) if cfg.get("groovy_needed") else ""
                zip_bytes = build_zip_from_skeleton(cfg["iflow_name"], cfg["iflow_name"], xml, gcode)
                groovy_code = gcode
                fname = f"{safe_slug(cfg['iflow_name'])}.zip"
                iname = cfg["iflow_name"]; iop = cfg["operation"]; ient = cfg["entity_name"]
                reply = f"Skeleton iFlow: `{iname}` | `{iop}` | `{ient}`"
                if show_d:
                    reply += "\n\n---\n\n## Flow Diagram\n\n" + generate_flow_diagram_text(cfg)
            else:
                with st.spinner("Generating your iFlow..."):
                    zip_bytes, groovy_code, summary = generate_iflow(cfg, index,
                                                                       preferred_template=pref)
                fname = f"{safe_slug(cfg['iflow_name'])}.zip"
                reply = summary
                if groovy_code:
                    gop = cfg["operation"]; gent = cfg["entity_name"]
                    reply += f"\n\nGroovy: `{gop}` pattern for `{gent}`"
                if show_d:
                    reply += "\n\n---\n\n## Flow Diagram\n\n" + generate_flow_diagram_text(cfg)

            # Append template picker hint
            index_now = load_index()
            if index_now:
                reply += "\n\n---\n\n Not working? Use Show all trained templates in the sidebar."

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
                placeholder="e.g. GET iFlow for Purchase Orders · ABAP for commitment data · Explain Process Order",
            )
        with col_btn:
            submitted = st.form_submit_button("Send", use_container_width=True)

    if submitted and user_input.strip():
        prompt = user_input.strip()
        st.session_state.messages.append({"role":"user","content":prompt})

        # ── FUNCTIONAL QUESTION — answer from trained docs ─────────────────
        # ── ABAP: code or concept ────────────────────────────────────────
        if is_abap_request(prompt):
            with st.spinner("Generating ABAP response…"):
                abap_resp = _web_answer_abap(prompt)
            st.session_state.messages.append({"role":"assistant","content":abap_resp})
            st.rerun()
            st.stop()  # ← prevent any iFlow code below from running

        # ── FUNCTIONAL Q&A: Extended KB v2 → original KB → web ────────
        elif is_functional_question(prompt):
            with st.spinner("Finding the best answer…"):
                qa_resp = answer_functional_v2(prompt)
            st.session_state.messages.append({"role":"assistant","content":qa_resp})
            st.rerun()
            st.stop()  # ← prevent any iFlow code below from running

        elif smartapp_prompt_requested(prompt):
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
                    "role": "assistant",
                    "content": (
                        f"⚠️ Please fill in **{', '.join(missing_fields)}** in the "
                        "**⚙️ Smartapp ONE Package Inputs** section in the sidebar, "
                        "then send the prompt again."
                    )
                })
                st.rerun()
                st.stop()
            with st.spinner("Generating Smartapp ONE package…"):
                try:
                    zip_bytes, summary = generate_smartapp_package(prompt, replacements_override=replacements_override)
                except FileNotFoundError as e:
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": (
                            f"⚠️ **Smartapp ONE package not found.**\n\n{e}\n\n"
                            "Upload the Smartapp ONE ZIP via **📁 Upload iFlows** and train it first."
                        )
                    })
                    st.rerun()
                    st.stop()
            fname = "Smartapp_ONE_modified.zip"
            zkey  = f"zip_{int(time.time()*1000)}"
            st.session_state[zkey] = zip_bytes
            st.session_state.messages.append({
                "role":"assistant","content":summary,
                "has_zip":True,"zip_key":zkey,"zip_fname":fname,
            })
            (OUTPUT_DIR / fname).write_bytes(zip_bytes)
            st.rerun()

        # ── iFlow generation (only if NOT handled above) ─────────────────
        else:

         cfg        = parse_intent(prompt)
         index      = load_index()
         pref       = st.session_state.get("preferred_template","").strip()
         mode_s     = st.session_state.get("iflow_mode","auto")
         show_d     = st.session_state.get("show_diagram", True)
         zkey       = f"zip_{int(time.time()*1000)}"

         # Determine generation mode
         use_dynamic = (
             mode_s == "dynamic" or
             (mode_s == "auto" and is_dynamic_iflow_request(prompt))
         )

        if not index and mode_s == "template":
            st.warning("⚠ No trained index found. Switching to Dynamic mode. Train your index for template mode.")
            use_dynamic = True

        # ── Mode: Dynamic ──────────────────────────────────────────────────
        if use_dynamic:
            thinking = st.empty()
            with thinking:
                st.info("✨ **Dynamic mode** — building iFlow from your description…")
            with st.spinner("Analysing scenario and generating iFlow + Groovy…"):
                zip_bytes, groovy_code, summary, flow_diagram = generate_dynamic_iflow(
                    prompt, index, preferred_template=pref)
            thinking.empty()
            fname    = f"{safe_slug(cfg['iflow_name'])}.zip"
            reply    = summary
            if show_d:
                reply += "\n\n---\n\n## Flow Diagram\n\n" + flow_diagram
            gc_prev = groovy_code[:1800] + ("..." if len(groovy_code)>1800 else "")
            reply += "\n\n---\n\n**Groovy Script:**\n\n```groovy\n" + gc_prev + "\n```"

        # Mode: Skeleton
        elif mode_s == "skeleton":
            xml = SKELETONS.get(cfg["operation"], SKELETONS["GET"])
            for ph, val in [("%%IFLOW_NAME%%", cfg["iflow_name"]),
                             ("%%SENDER_PATH%%", cfg["sender_path"]),
                             ("%%ENTITY_NAME%%", cfg["entity_name"]),
                             ("%%ODATA_ADDRESS%%", cfg.get("odata_address", ""))]:
                xml = xml.replace(ph, val)
            gcode = get_groovy(cfg["operation"], cfg["entity_name"], cfg["sender_path"]) if cfg.get("groovy_needed") else ""
            zip_bytes = build_zip_from_skeleton(cfg["iflow_name"], cfg["iflow_name"], xml, gcode)
            groovy_code = gcode
            fname = f"{safe_slug(cfg['iflow_name'])}.zip"
            iname = cfg["iflow_name"]; iop = cfg["operation"]; ient = cfg["entity_name"]
            reply = f"Skeleton iFlow: `{iname}` | `{iop}` | `{ient}`"
            if show_d:
                reply += "\n\n---\n\n## Flow Diagram\n\n" + generate_flow_diagram_text(cfg)

        # Mode: Template / Auto-template
        else:
            thinking = st.empty()
            with thinking:
                entity_str = cfg["entity_name"] or "entity"
                st.info(f"Building {cfg['operation']} iFlow for `{entity_str}` from {len(index)} templates...")
            with st.spinner("Building iFlow from template..."):
                zip_bytes, groovy_code, summary = generate_iflow(cfg, index,
                                                                   preferred_template=pref)
            thinking.empty()
            fname = f"{safe_slug(cfg['iflow_name'])}.zip"
            reply = summary
            if groovy_code:
                gop = cfg["operation"]; gent = cfg["entity_name"]
                reply += f"\n\nGroovy: `{gop}` pattern for `{gent}`"
            if show_d:
                reply += "\n\n---\n\n## Flow Diagram\n\n" + generate_flow_diagram_text(cfg)

        # Append template picker hint
        if index:
            reply += "\n\n---\n\nNot working? Use Show all trained templates in sidebar."


        st.session_state[zkey] = zip_bytes
        st.session_state.messages.append({
            "role":"assistant","content":reply,
            "has_zip":True,"zip_key":zkey,"zip_fname":fname,
        })
        (OUTPUT_DIR / fname).write_bytes(zip_bytes)
        st.rerun()

    if not st.session_state.messages:
        # ── Welcome screen — native Streamlit components (no raw HTML grid) ──
        st.markdown(
            "<div style='text-align:center;padding:30px 0 20px 0;'>"
            "<span style='font-size:48px;'>⚡</span><br>"
            "<span style='font-size:22px;font-weight:700;color:#e6edf3;'>"
            "SAP Intelligence Suite v2</span><br>"
            "<span style='font-size:13px;color:#8b949e;'>"
            "CPI iFlow Generator  ·  ABAP & RAP Code Generator  ·  Functional Consulting Knowledge"
            "</span></div>",
            unsafe_allow_html=True,
        )
        st.markdown("---")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**⚡ CPI iFlow Generation**")
            st.caption(
                "💬 GET iFlow for Purchase Orders with Groovy\n\n"
                "💬 POST iFlow to create Sales Order\n\n"
                "💬 DELETE iFlow for A_PurchaseOrder\n\n"
                "💬 Generate New Smartapp ONE Package"
            )

        with col2:
            st.markdown("**📝 ABAP & RAP Code**")
            st.caption(
                "💬 ABAP program for financial commitment pooling from PO and Budget\n\n"
                "💬 CDS View for Purchase Orders\n\n"
                "💬 RAP model for Sales Orders with OData V4\n\n"
                "💬 ABAP class for Material Master"
            )

        with col3:
            st.markdown("**🎓 Functional Knowledge**")
            st.caption(
                "💬 Explain Process Order in SAP\n\n"
                "💬 What is Financial Commitment in SAP FM?\n\n"
                "💬 Explain Order to Cash end-to-end\n\n"
                "💬 What is the Procure to Pay process?"
            )

        st.markdown("---")
        st.info(
            "💡 **How to use:** Type your request below, or expand the "
            "**Quick Prompts** in the sidebar to get started instantly.",
            icon="💡",
        )


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


# ═══════════════════════════════════════════════════════════════════════════════
# 📚 TRAIN DOCS — Upload & Index Functional Documents
# ═══════════════════════════════════════════════════════════════════════════════

elif mode == "📚 Train Docs":
    st.markdown("## 📚 Train Functional Documents")
    st.info(
        "Upload your SAP functional documents — **PDF, Word (.docx), Text, Markdown**. "
        "Once trained, ask questions like *'What is a Sales Order?'* or "
        "*'Explain the Purchase to Pay process'* directly in the chat."
    )

    # ── Upload section ────────────────────────────────────────────────────────
    st.markdown("### Step 1 — Upload Documents")
    uploaded_docs = st.file_uploader(
        "Drop your documents here",
        type=["pdf", "docx", "doc", "txt", "md", "csv"],
        accept_multiple_files=True,
        key="doc_uploader",
        help="Supports PDF, Word documents, plain text and markdown files.",
    )

    if uploaded_docs:
        st.success(f"✓ {len(uploaded_docs)} file(s) selected")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Selected files:**")
            for f in uploaded_docs:
                size_kb = round(len(f.getvalue()) / 1024, 1)
                ext = Path(f.name).suffix.lower()
                icon = {"pdf": "📕", "docx": "📘", "doc": "📘",
                        "txt": "📄", "md": "📄", "csv": "📊"}.get(ext.lstrip("."), "📄")
                st.markdown(f"{icon} `{f.name}` — {size_kb} KB")

    st.markdown("---")

    # ── Optional: paste text directly ────────────────────────────────────────
    st.markdown("### Step 2 — Or Paste Text Directly")
    paste_name = st.text_input(
        "Document name (for pasted text)",
        placeholder="e.g. Sales_Order_Overview.txt",
        key="doc_paste_name",
    )
    paste_text = st.text_area(
        "Paste document content here",
        height=180,
        placeholder="Paste any SAP functional documentation, process descriptions, field lists...",
        key="doc_paste_text",
    )

    st.markdown("---")

    # ── Train button ──────────────────────────────────────────────────────────
    st.markdown("### Step 3 — Train")
    if st.button("🧠 Train Documents Now", type="primary", use_container_width=True):

        # Build file list from uploads + pasted text
        files_to_train = []

        if uploaded_docs:
            for uf in uploaded_docs:
                raw = uf.getvalue()
                # Save to docs_library for reference
                (DOCS_DIR / uf.name).write_bytes(raw)
                files_to_train.append({"name": uf.name, "bytes": raw})

        if paste_text.strip() and paste_name.strip():
            pname = paste_name.strip()
            if not pname.endswith((".txt", ".md")):
                pname += ".txt"
            raw = paste_text.strip().encode("utf-8")
            (DOCS_DIR / pname).write_bytes(raw)
            files_to_train.append({"name": pname, "bytes": raw})

        if not files_to_train:
            st.warning("Please upload at least one document or paste some text first.")
            st.stop()

        with st.spinner(f"Reading and indexing {len(files_to_train)} document(s)…"):
            chunks_added, files_ok, errors = train_docs(files_to_train)

        if files_ok > 0:
            st.success(
                f"✅ Training complete! "
                f"**{files_ok}** document(s) indexed — "
                f"**{chunks_added}** searchable chunks created."
            )
        if errors:
            with st.expander(f"⚠ {len(errors)} issue(s)"):
                for e in errors:
                    st.text(e)

        if files_ok > 0:
            st.info("👉 Switch to **💬 Chat & Generate** and ask *'What is a Sales Order?'*")

    # ── Show current docs index ───────────────────────────────────────────────
    st.markdown("---")
    existing_docs = sorted(DOCS_DIR.glob("*"))
    docs_idx      = load_docs_index()

    col_a, col_b = st.columns(2)
    col_a.metric("Documents in library", len(existing_docs))
    col_b.metric("Searchable chunks",    len(docs_idx))

    if existing_docs:
        st.markdown("**Trained documents:**")
        for dp in existing_docs:
            size_kb = round(dp.stat().st_size / 1024, 1)
            ext  = dp.suffix.lower()
            icon = {"pdf": "📕", "docx": "📘", "doc": "📘",
                    "txt": "📄", "md": "📄", "csv": "📊"}.get(ext.lstrip("."), "📄")
            # Count chunks for this doc
            n_chunks = sum(1 for r in docs_idx if r.get("source") == dp.name)
            st.markdown(f"{icon} `{dp.name}` — {size_kb} KB — {n_chunks} chunks")

    if docs_idx and st.button("🗑 Clear Docs Index", use_container_width=True):
        DOCS_INDEX.unlink(missing_ok=True)
        for dp in DOCS_DIR.glob("*"):
            dp.unlink()
        st.success("Docs index cleared.")
        st.rerun()

    # ── Test search ───────────────────────────────────────────────────────────
    if docs_idx:
        st.markdown("---")
        st.markdown("### Test Search")
        test_q = st.text_input(
            "Try a question to test the index",
            placeholder="e.g. What is a Sales Order?",
            key="doc_test_q",
        )
        if test_q.strip():
            hits = search_docs(test_q, top_k=3)
            if hits:
                st.success(f"✓ Found {len(hits)} relevant chunks")
                for h in hits:
                    with st.expander(f"Score {h['score']} — {h['source']} (chunk {h['chunk_id']})"):
                        st.write(h["text"][:600])
            else:
                st.warning("No relevant chunks found. Try different keywords.")