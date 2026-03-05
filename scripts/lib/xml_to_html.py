"""Convert CNH Arbortext XML to HTML using declarative tag mapping.

Core design: recursive _convert_element(elem, ctx) walks the XML tree,
producing HTML strings. Tail text is handled carefully to preserve
inline text sequences like `<Part_Set>X-950</Part_Set> (CN1A)`.
"""

import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from html import escape
from pathlib import Path

import yaml

_TAG_MAP = None
_UNKNOWN_TAGS = defaultdict(int)


def _load_tag_map():
    """Load tag_map.yaml (cached)."""
    global _TAG_MAP
    if _TAG_MAP is not None:
        return _TAG_MAP
    config_path = Path(__file__).resolve().parent.parent.parent / "config" / "tag_map.yaml"
    with open(config_path) as f:
        _TAG_MAP = yaml.safe_load(f)
    return _TAG_MAP


def reset_unknown_tags():
    """Clear the unknown tags counter."""
    _UNKNOWN_TAGS.clear()


def get_unknown_tags():
    """Return dict of {tag: count} for tags not in tag_map."""
    return dict(_UNKNOWN_TAGS)


def _strip_processing_instructions(xml_str: str) -> str:
    """Remove Arbortext processing instructions like <?Pub ...?>."""
    return re.sub(r'<\?Pub[^?]*\?>', '', xml_str)


def convert(xml_str: str) -> str:
    """Convert an XML string to HTML.

    Returns the HTML string. Unknown tags are logged to the internal counter.
    """
    tag_map = _load_tag_map()
    cleaned = _strip_processing_instructions(xml_str)

    try:
        root = ET.fromstring(cleaned)
    except ET.ParseError:
        # Try wrapping in a root element if it fails
        try:
            root = ET.fromstring(f"<_root>{cleaned}</_root>")
        except ET.ParseError as e:
            return f"<!-- XML parse error: {e} -->"

    ctx = {}
    html = _convert_element(root, ctx, tag_map)
    return html.strip()


def convert_tree(root: ET.Element) -> str:
    """Convert a pre-parsed XML Element tree to HTML."""
    tag_map = _load_tag_map()
    ctx = {}
    return _convert_element(root, ctx, tag_map).strip()


def _make_attrs_str(mapping: dict, static_attrs: dict = None, elem=None, attrs_from: dict = None):
    """Build an HTML attributes string from static attrs and mapped attrs."""
    parts = {}
    if static_attrs:
        parts.update(static_attrs)
    if attrs_from and elem is not None:
        for html_attr, xml_attr in attrs_from.items():
            val = elem.get(xml_attr)
            if val:
                parts[html_attr] = val
    if not parts:
        return ""
    return " " + " ".join(f'{k}="{escape(str(v))}"' for k, v in parts.items())


def _convert_children(elem, ctx, tag_map):
    """Convert all children of an element, returning HTML string.

    Handles list_context wrapping: consecutive children with list_context
    are grouped into <ol> or <ul>.
    """
    parts = []
    # First, convert all children to (html, list_context, tag) tuples
    child_results = []
    for child in elem:
        child_html = _convert_element(child, ctx, tag_map)
        tail = escape(child.tail) if child.tail else ""

        spec = tag_map.get(child.tag, {})
        lc = spec.get("list_context")
        child_results.append((child_html, tail, lc))

    # Group consecutive list_context items
    i = 0
    while i < len(child_results):
        html, tail, lc = child_results[i]
        if lc:
            # Start a list group
            list_type = lc  # "ol" or "ul"
            group = [html]
            between = []  # non-list items that appear between list items
            j = i + 1
            # Collect tail of first item
            first_tail = tail

            while j < len(child_results):
                next_html, next_tail, next_lc = child_results[j]
                if next_lc == list_type:
                    # If there were non-list items between, flush previous group
                    if between:
                        parts.append(f"<{list_type}>{''.join(group)}</{list_type}>")
                        parts.extend(between)
                        group = []
                        between = []
                    group.append(next_html)
                    tail = next_tail
                    j += 1
                elif next_lc:
                    # Different list type, break
                    break
                else:
                    # Non-list item — could be between list items
                    # Check if there are more list items after
                    has_more = any(
                        child_results[k][2] == list_type
                        for k in range(j + 1, min(j + 5, len(child_results)))
                    )
                    if has_more:
                        between.append(next_html + next_tail)
                        j += 1
                    else:
                        break

            parts.append(f"<{list_type}>{''.join(group)}</{list_type}>")
            if first_tail.strip():
                parts.append(first_tail)
            parts.extend(between)
            if tail.strip():
                parts.append(tail)
            i = j
        else:
            parts.append(html + tail)
            i += 1

    return "".join(parts)


def _convert_element(elem, ctx, tag_map):
    """Convert a single element to HTML.

    Returns the HTML string for this element (not including tail text,
    which is handled by the parent).
    """
    tag = elem.tag
    spec = tag_map.get(tag)

    if spec is None:
        # Unknown tag — unwrap (use children) and log
        _UNKNOWN_TAGS[tag] += 1
        inner = _get_inner(elem, ctx, tag_map)
        return inner

    # Strip: remove element and all children
    if spec.get("strip"):
        return ""

    # Handler: delegate to special-case function
    handler = spec.get("handler")
    if handler:
        handler_fn = _HANDLERS.get(handler)
        if handler_fn:
            return handler_fn(elem, ctx, tag_map)
        # Unknown handler, fall through to default
        _UNKNOWN_TAGS[f"handler:{handler}"] += 1

    # Unwrap: replace with content
    if spec.get("unwrap"):
        return _get_inner(elem, ctx, tag_map)

    # Standard HTML tag mapping
    html_tag = spec.get("html_tag", "div")
    attrs_str = _make_attrs_str(
        spec, spec.get("attrs"), elem, spec.get("attrs_from")
    )
    prefix = spec.get("prefix", "")
    suffix = spec.get("suffix", "")
    inner = _get_inner(elem, ctx, tag_map)

    return f"<{html_tag}{attrs_str}>{prefix}{inner}{suffix}</{html_tag}>"


def _get_inner(elem, ctx, tag_map):
    """Get the inner content of an element: its text + converted children."""
    text = escape(elem.text) if elem.text else ""
    children_html = _convert_children(elem, ctx, tag_map)
    return text + children_html


# ── Special-case handlers ──

def _handle_tgroup(elem, ctx, tag_map):
    """Handle CALS tgroup: build colname→index map for colspan."""
    # Build column index from colspec children
    col_index = {}
    col_pos = 0
    for child in elem:
        if child.tag == "colspec":
            name = child.get("colname", f"col{col_pos}")
            col_index[name] = col_pos
            col_pos += 1

    old_col_index = ctx.get("col_index")
    ctx["col_index"] = col_index

    inner = _get_inner(elem, ctx, tag_map)

    # Restore previous col_index (for nested tables)
    if old_col_index is not None:
        ctx["col_index"] = old_col_index
    else:
        ctx.pop("col_index", None)

    return inner


def _handle_entry(elem, ctx, tag_map):
    """Handle CALS table entry with colspan support."""
    in_thead = ctx.get("in_thead", False)
    cell_tag = "th" if in_thead else "td"

    attrs = {}
    # Colspan from namest/nameend
    namest = elem.get("namest")
    nameend = elem.get("nameend")
    col_index = ctx.get("col_index", {})
    if namest and nameend and namest in col_index and nameend in col_index:
        span = col_index[nameend] - col_index[namest] + 1
        if span > 1:
            attrs["colspan"] = str(span)

    # Alignment
    align = elem.get("align")
    if align:
        attrs["style"] = f"text-align: {align}"

    valign = elem.get("valign")
    if valign:
        existing = attrs.get("style", "")
        attrs["style"] = f"{existing}; vertical-align: {valign}".lstrip("; ")

    attrs_str = ""
    if attrs:
        attrs_str = " " + " ".join(f'{k}="{escape(v)}"' for k, v in attrs.items())

    inner = _get_inner(elem, ctx, tag_map)
    return f"<{cell_tag}{attrs_str}>{inner}</{cell_tag}>"


def _handle_list_tag(elem, ctx, tag_map):
    """Handle the List tag — check ordered attr to decide ol vs ul."""
    ordered = elem.get("ordered", "").lower()
    labeltype = elem.get("labeltype", "").lower()

    if ordered == "yes" or labeltype in ("number", "arabic", "roman"):
        list_tag = "ol"
    else:
        list_tag = "ul"

    inner = _get_inner(elem, ctx, tag_map)
    return f"<{list_tag}>{inner}</{list_tag}>"


def _handle_image(elem, ctx, tag_map):
    """Handle Image/Illustration: extract fileref from config child."""
    fileref = None
    name = None

    # Look for config child with fileref
    for child in elem:
        if child.tag.endswith("_Config") or child.tag == "Image_Config":
            fr = child.get("fileref")
            if fr:
                fileref = fr
            n = child.get("name")
            if n:
                name = n
            # Also check Icon children for fileref
            for grandchild in child:
                if grandchild.tag == "Icon":
                    fr = grandchild.get("fileref")
                    if fr:
                        fileref = fr

    if not fileref:
        # Try the element itself
        fileref = elem.get("fileref", "")

    alt = name or fileref or "image"
    if fileref:
        return f'<img src="{escape(fileref)}" alt="{escape(alt)}" />'
    return f"<!-- image: no fileref found -->"


def _handle_icon(elem, ctx, tag_map):
    """Handle Icon element with fileref."""
    fileref = elem.get("fileref", "")
    if fileref:
        return f'<img src="{escape(fileref)}" alt="icon" class="icon" />'
    return ""


def _handle_physical_data(elem, ctx, tag_map):
    """Handle Physical_Data: render value + unit inline."""
    value = ""
    unit = ""
    for child in elem.iter("Value"):
        value = _text_content(child)
        break
    for child in elem.iter("Unit"):
        unit = _text_content(child)
        break
    if value and unit:
        return f"{escape(value)}\u00a0{escape(unit)}"
    return escape(value or _text_content(elem))


def _handle_physical_data_mi(elem, ctx, tag_map):
    """Handle Physical_Data_MI: metric only (drop imperial)."""
    # Find Physical_Data_M child
    for child in elem:
        if child.tag in ("Physical_Data_M", "Range_M"):
            return _convert_element(child, ctx, tag_map)
    # Fallback: try to get metric value directly
    return _handle_physical_data(elem, ctx, tag_map)


def _handle_physical_data_value(elem, ctx, tag_map):
    """Handle Physical_Data_M or Range_M: value + unit."""
    return _handle_physical_data(elem, ctx, tag_map)


def _handle_range(elem, ctx, tag_map):
    """Handle Range: min–max unit."""
    min_val = ""
    max_val = ""
    unit = ""
    for child in elem.iter("Min_Value"):
        min_val = _text_content(child)
        break
    for child in elem.iter("Max_Value"):
        max_val = _text_content(child)
        break
    for child in elem.iter("Unit"):
        unit = _text_content(child)
        break
    if min_val and max_val:
        result = f"{escape(min_val)}\u2013{escape(max_val)}"
        if unit:
            result += f"\u00a0{escape(unit)}"
        return result
    return escape(_text_content(elem))


def _handle_range_mi(elem, ctx, tag_map):
    """Handle Range_MI: metric only."""
    for child in elem:
        if child.tag == "Range_M":
            return _handle_range(child, ctx, tag_map)
    return _handle_range(elem, ctx, tag_map)


def _handle_range_value(elem, ctx, tag_map):
    """Handle Range_M: min–max unit."""
    return _handle_range(elem, ctx, tag_map)


def _handle_tolerance(elem, ctx, tag_map):
    """Handle Tolerance: value ± tolerance."""
    parts = []
    for child in elem:
        parts.append(_text_content(child))
    return escape(" ".join(parts)) if parts else escape(_text_content(elem))


def _text_content(elem):
    """Get all text from element and descendants."""
    return "".join(elem.itertext()).strip()


# Override _convert_element for thead to set in_thead context
_orig_convert_children = _convert_children


def _convert_children_with_thead(elem, ctx, tag_map):
    """Wrapper that sets in_thead for thead elements."""
    parts = []
    child_results = []

    for child in elem:
        if child.tag == "thead":
            old = ctx.get("in_thead")
            ctx["in_thead"] = True
            child_html = _convert_element(child, ctx, tag_map)
            ctx["in_thead"] = old if old is not None else False
            if "in_thead" in ctx and not ctx["in_thead"]:
                del ctx["in_thead"]
        else:
            child_html = _convert_element(child, ctx, tag_map)

        tail = escape(child.tail) if child.tail else ""
        spec = tag_map.get(child.tag, {})
        lc = spec.get("list_context")
        child_results.append((child_html, tail, lc))

    # Now do the list_context grouping (same logic as _convert_children)
    i = 0
    while i < len(child_results):
        html, tail, lc = child_results[i]
        if lc:
            list_type = lc
            group = [html]
            between = []
            first_tail = tail
            j = i + 1
            tail = first_tail

            while j < len(child_results):
                next_html, next_tail, next_lc = child_results[j]
                if next_lc == list_type:
                    if between:
                        parts.append(f"<{list_type}>{''.join(group)}</{list_type}>")
                        parts.extend(between)
                        group = []
                        between = []
                    group.append(next_html)
                    tail = next_tail
                    j += 1
                elif next_lc:
                    break
                else:
                    has_more = any(
                        child_results[k][2] == list_type
                        for k in range(j + 1, min(j + 5, len(child_results)))
                    )
                    if has_more:
                        between.append(next_html + next_tail)
                        j += 1
                    else:
                        break

            parts.append(f"<{list_type}>{''.join(group)}</{list_type}>")
            if first_tail.strip():
                parts.append(first_tail)
            parts.extend(between)
            if tail.strip() and tail != first_tail:
                parts.append(tail)
            i = j
        else:
            parts.append(html + tail)
            i += 1

    return "".join(parts)


# Replace _convert_children with thead-aware version
_convert_children = _convert_children_with_thead


# Handler registry
_HANDLERS = {
    "tgroup": _handle_tgroup,
    "entry": _handle_entry,
    "list_tag": _handle_list_tag,
    "image": _handle_image,
    "icon": _handle_icon,
    "physical_data": _handle_physical_data,
    "physical_data_mi": _handle_physical_data_mi,
    "physical_data_value": _handle_physical_data_value,
    "range": _handle_range,
    "range_mi": _handle_range_mi,
    "range_value": _handle_range_value,
    "tolerance": _handle_tolerance,
}
