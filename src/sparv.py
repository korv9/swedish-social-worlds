"""Stream and parse Sparv-annotated corpus XML from Språkbanken (Folke, Litteraturbanken)."""
import bz2
import urllib.request
import xml.etree.ElementTree as ET

CONTENT_POS = {"NN", "VB", "JJ"}  # proper names (PM) excluded: they made keywords and clusters track books
STOP_LEMMAS = {"vara", "ha", "skola", "bli", "kunna", "vilja", "få", "måste", "böra", "komma", "göra", "säga"}
NAME_MASK = "[namn]"


def stream_texts(url, max_texts, keep=lambda attrs: True):
    """Yield (attrs, raw_xml) for the first `max_texts` <text> elements accepted by `keep`.

    Streams the .bz2 over HTTP and stops early, so a 5.75 GB corpus costs only what we read.
    """
    # ponytail: takes the first N texts in archive order, not a random sample; random needs a full scan.
    with urllib.request.urlopen(url) as resp, bz2.open(resp) as f:
        n = 0
        for _, elem in ET.iterparse(f, events=("end",)):
            if elem.tag != "text":
                continue
            if keep(elem.attrib):
                yield dict(elem.attrib), ET.tostring(elem, encoding="unicode")
                n += 1
            elem.clear()
            if n >= max_texts:
                return


def _tail(token):
    # Sparv stores trailing whitespace escaped in `_tail`; no attribute means no space.
    return token.get("_tail", "").replace("\\s", " ").replace("\\n", "\n")


def text_id(attrs):
    return attrs.get("id") or attrs.get("lbworkid") or attrs["_id"]


def sentences(raw_xml):
    """Yield (text, masked_text, content_lemmas, page_url) per sentence, in document order.

    masked_text replaces each run of proper-name tokens with NAME_MASK, so embeddings group by theme, not by
    character names. Accepts an XML string or Element.
    """
    root = ET.fromstring(raw_xml) if isinstance(raw_xml, str) else raw_xml
    page_url = None
    for el in root.iter():
        if el.tag == "page":
            page_url = el.get("url") or page_url
        elif el.tag == "sentence":
            # Pages can also start inside a sentence (Sparv splits overlapping spans).
            inner = el.find(".//page")
            if inner is not None:
                page_url = inner.get("url") or page_url
            toks = list(el.iter("token"))
            text = " ".join("".join((t.text or "") + _tail(t) for t in toks).split())
            masked, prev_pm = [], False
            for t in toks:
                pm = t.get("pos") == "PM"
                if pm and prev_pm:
                    masked[-1] = NAME_MASK + _tail(t)  # one mask per run of names ("Per Ludvig Dahlström")
                else:
                    masked.append((NAME_MASK if pm else t.text or "") + _tail(t))
                prev_pm = pm
            lemmas = [
                t.get("lemma", "|").strip("|").split("|")[0].lower()
                for t in toks
                if t.get("pos") in CONTENT_POS
            ]
            yield (text, " ".join("".join(masked).split()),
                   [l for l in lemmas if len(l) > 1 and l not in STOP_LEMMAS], page_url)


def passages(raw_xml, target_tokens=150):
    """Group consecutive sentences into passages of ~target_tokens whitespace tokens.

    Yields (text, masked_text, lemmas, page_url_at_start).
    """
    buf, mbuf, lemmas, start_url, n = [], [], [], None, 0
    for text, masked, lem, url in sentences(raw_xml):
        if not buf:
            start_url = url
        buf.append(text)
        mbuf.append(masked)
        lemmas += lem
        n += len(text.split())
        if n >= target_tokens:
            yield " ".join(buf), " ".join(mbuf), lemmas, start_url
            buf, mbuf, lemmas, n = [], [], [], 0
    if buf:
        yield " ".join(buf), " ".join(mbuf), lemmas, start_url


if __name__ == "__main__":
    lb = """<text title="T"><sentence><page n="1" url="u1"/>
      <token pos="NN" lemma="|arbete|" _tail="\\s">Arbetet</token><token pos="VB" lemma="|vara|" _tail="\\s">var</token>
      <ne><token pos="JJ" lemma="|tung|">tungt</token></ne><token pos="MAD" lemma="|">.</token></sentence>
      <page n="2" url="u2"/><sentence><token pos="NN" lemma="|piga|">Pigan</token></sentence></text>"""
    s = list(sentences(lb))
    assert s[0] == ("Arbetet var tungt.", "Arbetet var tungt.", ["arbete", "tung"], "u1"), s[0]
    assert s[1][3] == "u2"
    p = list(passages(lb, target_tokens=3))
    assert p[0] == ("Arbetet var tungt.", "Arbetet var tungt.", ["arbete", "tung"], "u1") and p[1][3] == "u2", p
    assert list(passages(lb))[0][0] == "Arbetet var tungt. Pigan"
    names = r"""<text><sentence><token pos="PM" _tail="\s">Per</token><token pos="PM" _tail="\s">Ludvig</token>
      <token pos="VB" lemma="|gå|" _tail="\s">gick</token><token pos="PP" _tail="\s">till</token>
      <token pos="PM">Lund</token><token pos="MAD">.</token></sentence></text>"""
    assert list(sentences(names))[0][:3] == ("Per Ludvig gick till Lund.", "[namn] gick till [namn].", ["gå"])
    print("ok")
