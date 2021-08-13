# CreatedBy: Emilia Crow
# CreateDate: 20210609
# Updated: 20210609
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject


class GSAPrice(BasicProcessObject):
    req_fields = ['IsVisible', 'DateCatalogReceived', 'GSABasePrice', 'GSAApprovedPriceDate', 'GSAPricingApproved','GSAContractNumber',
                  'GSAContractModificationNumber', 'GSA_IFFFeePercent', 'GSAProductGMPercent', 'GSAProductGMPrice', 'GSA_SIN']

    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'GSA Price Ingestion'

    def process_product_line(self, return_df_line_product):
        return_df_line_product['Report'] = ['Process not built']
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():

            success, df_collect_product_base_data = self.process_contract(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process contract']
                return success, df_collect_product_base_data

        success, return_df_line_product = self.gsa_product_price(df_collect_product_base_data)

        return success, return_df_line_product

    def process_contract(self, df_collect_product_base_data, row):
        success = True
        contract_number = row['GSAContractNumber']
        contract_mod_number = row['GSAContractModificationNumber']
        if contract_number not in contract_mod_number:
            df_collect_product_base_data['Report'] = ['Contract numbers don\'t match']
            return False, df_collect_product_base_data

        return success, df_collect_product_base_data

    def gsa_product_price(self, df_line_product):
        success = True
        return_df_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            is_visible = row['IsVisible']
            date_catalog_received = row['DateCatalogReceived']
            sell_price = row['GSASellPrice']
            approved_price_date = row['GSAApprovedPriceDate']
            pricing_approved = row['GSAPricingApproved']
            contract_number = row['GSAContractNumber']
            contract_mod_number = row['GSAContractModificationNumber']
            iff_fee_precent = row['GSA_IFFFeePercent']
            product_gm_precent = row['GSAProductGMPercent']
            product_gm_price = row['GSAProductGMPrice']
            sin = row['GSA_SIN']


        gsa_price_id = self.obIngester.gsa_product_price_cap(is_visible,date_catalog_received,sell_price,approved_price_date,pricing_approved, contract_number,contract_mod_number,iff_fee_precent,product_gm_precent,product_gm_price,sin)
        if gsa_price_id != -1:
            return_df_line_product['GSAProductPriceId']=[gsa_price_id]
        else:
            return_df_line_product['FinalReport']=['Failed in GSA Price Ingestion']
            return False, return_df_line_product

        # this needs to be identified
        base_price_id = -1
        update_success = self.obIngester.update_fks_base_price(base_price_id, newGSAProductPriceId = gsa_price_id)
        if update_success == -1:
            return_df_line_product['FinalReport']=['Failed in Base Price update']
            return False, return_df_line_product

        return success, return_df_line_product


## end ##