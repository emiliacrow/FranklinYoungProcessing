# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject


class HTMEPrice(BasicProcessObject):
    req_fields = ['IsVisible', 'DateCatalogReceived', 'HTMESellPrice', 'HTMEApprovedPriceDate', 'HTMEPricingApproved', 'HTMEContractNumber',
                  'HTMEContractModificationNumber', 'HTMEProductGMPercent', 'HTMEProductGMPrice']

    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'HTME Price Ingestion'

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data = self.process_contract(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process contract']
                return success, df_collect_product_base_data

        success, return_df_line_product = self.htme_product_price(df_collect_product_base_data)

        return False, return_df_line_product

    def process_contract(self, df_collect_product_base_data, row):
        success = True
        contract_number = row['HTMEContractNumber']
        contract_mod_number = row['HTMEContractModificationNumber']
        if contract_number not in contract_mod_number:
            df_collect_product_base_data['Report'] = ['Contract numbers don\'t match']
            return False, df_collect_product_base_data

        return success, df_collect_product_base_data


    def htme_product_price(self, df_line_product):
        return_df_line_product = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            is_visible = row['IsVisible']
            date_catalog_received = row['DateCatalogReceived']
            sell_price = row['HTMESellPrice']
            approved_price_date = row['HTMEApprovedPriceDate']
            pricing_approved = row['HTMEPricingApproved']
            contract_number = row['HTMEContractNumber']
            contract_mod_number = row['HTMEContractModificationNumber']
            product_gm_precent = row['HTMEProductGMPercent']
            product_gm_price = row['HTMEProductGMPrice']

        htme_product_price_id = self.obIngester.htme_product_price_cap(is_visible,date_catalog_received,sell_price,approved_price_date,pricing_approved,contract_number,contract_mod_number,product_gm_precent,product_gm_price)
        if htme_product_price_id != -1:
            return_df_line_product['HTMEProductPriceId'] = [htme_product_price_id]
        else:
            return_df_line_product['FinalReport'] = ['Failed in HTME Price Ingestion']
            return False, return_df_line_product

        return True, return_df_line_product



## end ##