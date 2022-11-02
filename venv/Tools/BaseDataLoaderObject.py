# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210528
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class BaseDataLoader(BasicProcessObject):
    req_fields = []
    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Base Data Loader'


    def set_the_table(self,table_to_set):
        self.table_to_set = table_to_set

    def run_process(self):
        if self.table_to_set == 'Category':
            ingested_set = self.obIngester.ingest_categories(self.df_product)
            return_str = 'Ingested {} categories.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'Manufacturer':
            return_str = self.run_manufacturer_loads()

        elif self.table_to_set == 'Vendor':
            return_str = self.run_vendor_loads()

        elif self.table_to_set == 'Country':
            return_str = self.run_country_loads()

        elif self.table_to_set == 'UNSPSC Codes':
            ingested_set = self.obIngester.ingest_unspscs(self.df_product)
            return_str = 'Ingested {} UNSPSC codes.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'FSC Codes':
            ingested_set = self.obIngester.ingest_fscs(self.df_product)
            return_str = 'Ingested {} FSC codes.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'Hazardous Code':
            ingested_set = self.obIngester.ingest_hazard_codes(self.df_product)
            return_str = 'Ingested {} Hazardous codes.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'NAICS Code':
            ingested_set = self.obIngester.ingest_naics_codes(self.df_product)
            return_str = 'Ingested {} NAICS codes.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'Unit of Issue-Symbol':
            ingested_set = self.obIngester.ingest_uoi_symbols(self.df_product)
            return_str = 'Ingested {} uoi symbols.'.format(str(len(ingested_set)))

        elif self.table_to_set == 'Unit of Issue':
            ingested_set = self.obIngester.ingest_uois(self.df_product)
            return_str = 'Ingested {} uoi containers.'.format(str(len(ingested_set)))
        else:
            return False, 'No valid selection'

        return True, return_str


    def run_manufacturer_loads(self):
        df_new_manufacturers = self.df_product.copy()
        if self.df_manufacturer_translator is not None:
            df_new_manufacturers = pandas.merge(self.df_product, self.df_manufacturer_translator, how='outer',on=['SupplierName','ManufacturerName'],indicator=True)
            df_new_manufacturers = df_new_manufacturers[df_new_manufacturers['_merge']=='left_only']

        ingested_set = self.obIngester.ingest_manufacturers(df_new_manufacturers)
        return 'Ingested {} manufacturers.'.format(str(len(ingested_set)))


    def run_vendor_loads(self):
        df_new_vendors = self.df_product.copy()
        if self.df_vendor_translator is not None:
            df_new_vendors = pandas.merge(self.df_product, self.df_vendor_translator, how='outer',on=['VendorName','VendorCode'],indicator=True)
            df_new_vendors = df_new_vendors[df_new_vendors['_merge']=='left_only']

        ingested_set = self.obIngester.ingest_vendors(df_new_vendors)
        return 'Ingested {} vendors.'.format(str(len(ingested_set)))


    def run_country_loads(self):
        df_new_countries = self.df_product.copy()
        if self.df_country_translator is not None:
            df_new_countries = pandas.merge(self.df_product, self.df_country_translator, how='outer',on=['CountryCode','ECATCountryCode','CountryName','CountryLongName'],indicator=True)
            df_new_countries = df_new_countries[df_new_countries['_merge']=='left_only']

        ingested_set = self.obIngester.ingest_countries(df_new_countries)
        return 'Ingested {} countries.'.format(str(len(ingested_set)))





## end ##