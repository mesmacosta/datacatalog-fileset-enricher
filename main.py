import logging
from datacatalog_fileset_enricher import DatacatalogFilesetEnricherCLI

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    DatacatalogFilesetEnricherCLI.run()
