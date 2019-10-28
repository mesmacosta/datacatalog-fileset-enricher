import logging
import sys
from datacatalog_fileset_enricher import DatacatalogFilesetEnricherCLI

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    argv = sys.argv
    DatacatalogFilesetEnricherCLI.run(argv[1:] if len(argv) > 0 else argv)
