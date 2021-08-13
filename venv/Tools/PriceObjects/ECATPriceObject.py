# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject


class ECATPrice(BasicProcessObject):
    req_fields = ['IsVisible', 'DateCatalogReceived', 'ECATSellPrice','ECATApprovedPriceDate', 'ECATPricingApproved',  'ECATContractNumber',
                  'ECATContractModificationNumber', 'ECATProductGMPercent', 'ECATProductGMPrice',
                  'ECATMinimumQuantity']

    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'ECAT Price Ingestion'

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data = self.process_contract(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process contract']
                return success, df_collect_product_base_data

        success, return_df_line_product = self.ecat_product_price(df_collect_product_base_data)

        return False, return_df_line_product

    def process_contract(self, df_collect_product_base_data, row):
        success = True
        contract_number = row['ECATContractNumber']
        contract_mod_number = row['ECATContractModificationNumber']
        if contract_number not in contract_mod_number:
            df_collect_product_base_data['Report'] = ['Contract numbers don\'t match']
            return False, df_collect_product_base_data

        return success, df_collect_product_base_data


    def ecat_product_price(self, df_line_product):
        return_df_line_product = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            is_visible = row['IsVisible']
            date_catalog_received = row['DateCatalogReceived']
            sell_price = row['ECATSellPrice']
            approved_price_date = row['ECATApprovedPriceDate']
            pricing_approved = row['ECATPricingApproved']
            contract_number = row['ECATContractNumber']
            contract_mod_number = row['ECATContractModificationNumber']
            product_gm_precent = row['ECATProductGMPercent']
            product_gm_price = row['ECATProductGMPrice']
            minimum_quantity = row['ECATMinimumQuantity']

        ecat_product_price_id = self.obIngester.ecat_product_price_cap(is_visible,date_catalog_received,sell_price,approved_price_date,pricing_approved,contract_number,contract_mod_number,product_gm_precent,product_gm_price,minimum_quantity)
        if ecat_product_price_id != -1:
            return_df_line_product['ECATProductPriceId'] = [ecat_product_price_id]
        else:
            return_df_line_product['FinalReport'] = ['Failed in ECAT Price Ingestion']
            return False, return_df_line_product

        return True, return_df_line_product


## end ##