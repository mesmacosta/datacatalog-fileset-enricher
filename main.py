import logging
import sys
from datacatalog_fileset_enricher import datacatalog_fileset_enricher_cli

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    argv = sys.argv
    datacatalog_fileset_enricher_cli.\
        DatacatalogFilesetEnricherCLI.run(argv[1:] if len(argv) > 0 else argv)
