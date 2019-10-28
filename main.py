import logging
from datacatalog_fileset_enricher import datacatalog_fileset_enricher_cli

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run()
