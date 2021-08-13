# CreatedBy: Emilia Crow
# CreateDate: 20210602
# Updated: 20210608
# CreateFor: Franklin Young International


from Tools.BasicProcess import BasicProcessObject



class VAPrice(BasicProcessObject):
    req_fields = ['IsVisible',  'VASellPrice', 'DateCatalogReceived', 'VAPricingApproved', 'VAApprovedPriceDate', 'VAContractNumber',
                  'VAContractModificationNumber', 'VA_IFFFeePercent', 'VAProductGMPercent', 'VAProductGMPrice',
                  'VA_SIN']

    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'VA Price Ingestion'

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            success, df_collect_product_base_data = self.process_contract(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process contract']
                return success, df_collect_product_base_data

        success, return_df_line_product = self.va_product_price(df_collect_product_base_data)

        return False, return_df_line_product


    def process_contract(self, df_collect_product_base_data, row):
        success = True
        contract_number = row['VAContractNumber']
        contract_mod_number = row['VAContractModificationNumber']
        if contract_number not in contract_mod_number:
            df_collect_product_base_data['Report'] = ['Contract numbers don\'t match']
            return False, df_collect_product_base_data

        return success, df_collect_product_base_data

    def va_product_price(self, df_line_product):
        return_df_line_product = df_line_product.copy()


        for colName, row in df_line_product.iterrows():
            is_visible = row['IsVisible']
            date_catalog_received = row['DateCatalogReceived']
            sell_price = row['VASellPrice']
            approved_price_date = row['VAApprovedPriceDate']
            pricing_approved = row['VAPricingApproved']
            contract_number = row['VAContractNumber']
            contract_mod_number = row['VAContractModificationNumber']
            iff_fee_precent = row['VA_IFFFeePercent']
            product_gm_precent = row['VAProductGMPercent']
            product_gm_price = row['VAProductGMPrice']
            sin = row['VA_SIN']

        va_product_price_id = self.obIngester.va_product_price_cap(is_visible,date_catalog_received, sell_price, approved_price_date, pricing_approved, contract_number,contract_mod_number,iff_fee_precent,product_gm_precent,product_gm_price,sin)
        if va_product_price_id != -1:
            return_df_line_product['VAProductPriceId'] = [va_product_price_id]
        else:
            return_df_line_product['FinalReport'] = ['Failed in VA Price Ingestion']
            return False, return_df_line_product

        return True, return_df_line_product

