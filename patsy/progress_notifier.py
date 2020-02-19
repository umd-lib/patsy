class ProgressNotifier:
    """
    Default progress notifier, which does nothing
    """
    def notify(self, msg):
        """
        No-op
        :param msg: the notification message
        """
        pass


class PrintProgressNotifier(ProgressNotifier):
    """
    Progress notifier that prints to standard out
    """
    def notify(self, msg):
        """
        Prints the given message to standard out
        :param msg: the message to print
        """
        print(msg)
