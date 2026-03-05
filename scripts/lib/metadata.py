"""Extract structured metadata from raw CNH XML before HTML conversion.

Metadata is extracted from semantic XML tags that lose their meaning
after conversion to HTML. Run this on the raw parsed XML tree.
"""

import xml.etree.ElementTree as ET


def _text(elem):
    """Get all text content from an element and its descendants."""
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()


def _attr(elem, name, default=""):
    """Get an attribute, returning default if missing."""
    return elem.get(name, default)


def extract_metadata(root: ET.Element) -> dict:
    """Extract all metadata from a parsed XML tree.

    Returns dict with keys: fault_codes, part_numbers, tool_references,
    consumable_references, warranty_codes, iu_cross_references,
    configuration, fcr_chains.
    """
    return {
        "iu_type": root.tag,
        "fault_codes": _extract_fault_codes(root),
        "part_numbers": _extract_part_numbers(root),
        "tool_references": _extract_tool_references(root),
        "consumable_references": _extract_consumable_references(root),
        "warranty_codes": _extract_warranty_codes(root),
        "iu_cross_references": _extract_iu_cross_references(root),
        "configuration": _extract_configuration(root),
        "fcr_chains": _extract_fcr_chains(root),
    }


def _extract_fault_codes(root):
    """Extract from Fault_Code_Reference elements."""
    codes = []
    seen = set()
    for elem in root.iter("Fault_Code_Reference"):
        code = _attr(elem, "code")
        ecid = _attr(elem, "ecid")
        desc = _text(elem)
        key = (code, ecid)
        if key not in seen:
            seen.add(key)
            codes.append({"code": code, "ecid": ecid, "description": desc})
    return codes


def _extract_part_numbers(root):
    """Extract from Part_Reference elements.

    Part numbers are the text content of <Part_Reference> (e.g.,
    <Part_Reference>SGDF1</Part_Reference>), not in a child element.
    """
    parts = []
    seen = set()
    for ref in root.iter("Part_Reference"):
        number = _text(ref)
        if number and number not in seen:
            seen.add(number)
            parts.append({
                "part_number": number,
                "description": "",
            })
    return parts


def _extract_tool_references(root):
    """Extract from Tool_Reference elements.

    Part number is the text content, name is in the STName attribute.
    E.g., <Tool_Reference STName="Engine Turning Tool" STType="1">380000732</Tool_Reference>
    """
    tools = []
    seen = set()
    for elem in root.iter("Tool_Reference"):
        pn = _text(elem)
        name = _attr(elem, "STName")
        if pn and pn not in seen:
            seen.add(pn)
            tools.append({"part_number": pn, "name": name})
    return tools


def _extract_consumable_references(root):
    """Extract from Consumable_Reference elements."""
    items = []
    seen = set()
    for elem in root.iter("Consumable_Reference"):
        csid = _attr(elem, "csid")
        desc = _text(elem)
        if csid and csid not in seen:
            seen.add(csid)
            items.append({"csid": csid, "description": desc})
    return items


def _extract_warranty_codes(root):
    """Extract from Warranty_Code elements."""
    codes = []
    seen = set()
    for elem in root.iter("Warranty_Code"):
        code = _attr(elem, "code")
        time = _attr(elem, "time")
        key = (code, time)
        if key not in seen:
            seen.add(key)
            codes.append({"code": code, "time": time})
    return codes


def _extract_iu_cross_references(root):
    """Extract from IU_IFS_Link and IU_Reference elements."""
    refs = []
    seen = set()
    for tag in ("IU_IFS_Link", "IU_Reference"):
        for elem in root.iter(tag):
            icecode = _attr(elem, "icecode")
            ifsid = _attr(elem, "ifsid")
            key = (icecode, ifsid)
            if key not in seen:
                seen.add(key)
                refs.append({
                    "icecode": icecode,
                    "ifsid": ifsid,
                    "text": _text(elem),
                })
    return refs


def _extract_configuration(root):
    """Extract configdata values from Configuration elements."""
    configs = []
    seen = set()
    for elem in root.iter("Configuration"):
        configdata = _attr(elem, "configdata")
        if configdata and configdata not in seen:
            seen.add(configdata)
            configs.append(configdata)
    return configs


def _extract_fcr_chains(root):
    """Extract structured FCR (Fault Code Resolution) chains.

    Returns list of FCR chain dicts, each containing:
    - fault_code: {code, ecid, description}
    - ccm: text from Fault_Code_CCM
    - possible_failures: list of {cause, description}
    - tests: list of {number, testid, instruction_text,
                      results: [{result, action}]}
    - schematics: list of {icecode, ifsid}
    """
    chains = []
    for fcr in root.iter("Fault_Code_Resolution"):
        chain = {}

        # Fault code
        fc = fcr.find(".//Fault_Code_Reference")
        if fc is not None:
            chain["fault_code"] = {
                "code": _attr(fc, "code"),
                "ecid": _attr(fc, "ecid"),
                "description": _text(fc),
            }

        # CCM
        ccm = fcr.find(".//Fault_Code_CCM")
        if ccm is not None:
            chain["ccm"] = _text(ccm)

        # Possible failure modes
        failures = []
        for pfm in fcr.iter("FCR_Possible_Failure_Mode"):
            cause = pfm.find(".//FCR_Cause")
            desc = pfm.find(".//FCR_Description")
            failures.append({
                "cause": _text(cause) if cause is not None else "",
                "description": _text(desc) if desc is not None else "",
            })
        if failures:
            chain["possible_failures"] = failures

        # Tests
        tests = []
        for test in fcr.iter("FCR_Test"):
            t = {
                "number": _attr(test, "number"),
                "testid": _attr(test, "testid"),
                "instruction_text": "",
                "results": [],
            }
            # Get instruction text (everything not in result/action)
            instr_parts = []
            for child in test:
                if child.tag not in ("FCR_Result_Action", "FCR_Result", "FCR_Action"):
                    instr_parts.append(_text(child))
            t["instruction_text"] = " ".join(p for p in instr_parts if p)

            # Results
            for ra in test.iter("FCR_Result_Action"):
                result_elem = ra.find("FCR_Result")
                action_elem = ra.find("FCR_Action")
                t["results"].append({
                    "result": _text(result_elem) if result_elem is not None else "",
                    "action": _text(action_elem) if action_elem is not None else "",
                })
            tests.append(t)
        if tests:
            chain["tests"] = tests

        # Schematics
        schematics = []
        for sch in fcr.iter("FCR_Schematics"):
            for link in sch.iter("IU_IFS_Link"):
                schematics.append({
                    "icecode": _attr(link, "icecode"),
                    "ifsid": _attr(link, "ifsid"),
                })
        if schematics:
            chain["schematics"] = schematics

        if chain:
            chains.append(chain)

    return chains
