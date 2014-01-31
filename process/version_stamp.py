import os.path

cached_revision = None

def source_revision():
    global cached_revision

    if not cached_revision:
        toolsRootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        stompPath = os.path.join(toolsRootDir, '.version-stamp')
        if os.path.exists(stompPath):
            cached_revision = file(stompPath, "r").read().strip()
        else:
            cached_revision = 'unknown'
    return cached_revision
