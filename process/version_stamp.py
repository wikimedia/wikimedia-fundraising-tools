import os.path

cached_revision = None


def source_revision():
    global cached_revision

    if not cached_revision:
        toolsRootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        stampPath = os.path.join(toolsRootDir, '.version-stamp')
        if os.path.exists(stampPath):
            cached_revision = file(stampPath, "r").read().strip()
        else:
            cached_revision = 'unknown'
    return cached_revision
