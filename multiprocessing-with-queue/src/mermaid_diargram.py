import logging


class MermaidDiagram:
    logger = logging.getLogger("MermaidDiagram")

    def __init__(self, filename: str):
        self._filename = filename
        with open(self._filename, "w") as fd:
            fd.writelines("sequenceDiagram\n")
            self.logger.debug("sequenceDiagram")

    def append(self, line: str):
        with open(self._filename, "a") as fd:
            fd.writelines(f"    {line}\n")
            self.logger.debug(line)
