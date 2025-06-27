def remove_special_characters(text: str) -> str:
    """Remove special characters from the text."""
    out = ''.join(e for e in text if e.isalnum() or e.isspace()).strip()
    # remove multiple spaces
    out = ' '.join(out.split())
    # replace spaces with underscores
    out = out.replace(" ", "_")
    return out.lower()
