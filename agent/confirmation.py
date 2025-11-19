class ConfirmationManager:
    def __init__(self):
        self.pending = None

    def require_confirmation(self, tool_name, args):
        self.pending = {
            "tool": tool_name,
            "args": args
        }

    def clear(self):
        self.pending = None

    def has_pending(self):
        return self.pending is not None

    def get_pending(self):
        return self.pending
