# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class BigCommerceRTLObject(BasicProcessObject):
    req_fields = ['FyProductNumber','VendorPartNumber']

    sup_fields = ['BCPriceUpdateToggle','BCDataUpdateToggle','IsDiscontinued','AllowPurchases','IsVisible',
                  'UpdateImages','ECATOnContract','ECATPricingApproved','HTMETOnContract','HTMEPricingApproved',
                  'GSAOnContract','GSAPricingApproved','VAOnContract','VAPricingApproved']
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing,full_run=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Toggle Lifter'
        self.full_run = full_run

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()

    def define_new(self):
        # this should just try to gather the ID's of the relevant tables
        # and if don't get then they fail
        self.df_current_toggles = self.obDal.get_toggles()
        match_headers = ['FyProductNumber','VendorPartNumber']
        self.df_current_toggles['Filter'] = 'Update'
        self.df_product = self.df_product.merge(self.df_current_toggles, how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'Fail'

    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Fail' and not self.full_run:
                    self.obReporter.update_report('Alert','No change')
                    return False, df_collect_product_base_data
            else:
                self.obReporter.update_report('Alert','This product was processed anyway')
                return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product

    def process_changes(self, df_collect_product_base_data):
        
        for colName, row in df_collect_product_base_data.iterrows():
            product_id = row['ProductId']

            price_id = row['ProductPriceId']
            base_id = row['BaseProductPriceId']

            ecat_id = row['ECATProductPriceId']
            htme_id = row['HTMEProductPriceId']
            gsa_id = row['GSAProductPriceId']
            va_id = row['VAProductPriceId']

            fy_product_number = row['FyProductNumber']
            vendor_part_number = row['VendorPartNumber']
            
            price_toggle = -1
            data_toggle = -1
            if 'BCPriceUpdateToggle' in row:
                price_toggle = row['BCPriceUpdateToggle']
            if 'BCDataUpdateToggle' in row:
                data_toggle = row['BCDataUpdateToggle']

            is_discontinued = -1
            allow_purchases = -1
            
            if 'IsDiscontinued' in row:
                is_discontinued = row['IsDiscontinued']
            if 'AllowPurchases' in row:
                allow_purchases = row['AllowPurchases']
            
            is_visible = -1
            update_image = -1
            if 'IsVisible' in row:
                is_visible = row['IsVisible']
            if 'UpdateImages' in row:
                update_image = row['UpdateImages']
            
            ecat_contract = -1
            ecat_approved = -1
            if row['ECATProductPriceId'] != -1:
                if 'ECATOnContract' in row:
                    ecat_contract = row['ECATOnContract']
                if 'ECATPricingApproved' in row:
                    ecat_approved = row['ECATPricingApproved']
                
            htme_contract = -1
            htme_approved = -1
            if row['HTMEProductPriceId'] != -1:
                if 'HTMETOnContract' in row:
                    htme_contract = row['HTMETOnContract']
                if 'HTMEPricingApproved' in row:
                    htme_approved = row['HTMEPricingApproved']
                
            gsa_contract = -1
            gsa_approved = -1
            if row['GSAProductPriceId'] != -1:
                if 'GSAOnContract' in row:
                    gsa_contract = row['GSAOnContract']
                if 'GSAPricingApproved' in row:
                    gsa_approved = row['GSAPricingApproved']
                
            va_contract = -1
            va_approved = -1
            if row['VAProductPriceId'] != -1:
                if 'VAOnContract' in row:
                    va_contract = row['VAOnContract']
                if 'VAPricingApproved' in row:
                    va_approved = row['VAPricingApproved']


        if (price_toggle != -1 or data_toggle != -1):
            self.obIngester.set_bc_update_toggles(price_id, fy_product_number, price_toggle, data_toggle)

        if (is_discontinued != -1 or allow_purchases != -1):
            self.obIngester.set_is_discon_allow_purchase(price_id, fy_product_number, is_discontinued, allow_purchases)

        if (is_visible != -1):
            self.obIngester.set_is_visible(base_id, is_visible)

        if (update_image != -1):
            self.obIngester.set_update_image(product_id, update_image)

        # should we cascade the update toggles?

        if (ecat_contract != -1) or (ecat_approved != -1):
            self.obIngester.set_ecat_toggles(ecat_id, fy_product_number, ecat_contract, ecat_approved)

        if (htme_contract != -1) or (htme_approved != -1):
            self.obIngester.set_htme_toggles(htme_id, fy_product_number, htme_contract, htme_approved)

        if (gsa_contract != -1) or (gsa_approved != -1):
            self.obIngester.set_gsa_toggles(gsa_id, fy_product_number, gsa_contract, gsa_approved)

        if (va_contract != -1) or (va_approved != -1):
            self.obIngester.set_va_toggles(va_id, fy_product_number, va_contract, va_approved)


        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.set_bc_update_toggles_cleanup()
        self.obIngester.set_is_discon_allow_purchase_cleanup()
        self.obIngester.set_is_visible_cleanup()
        self.obIngester.set_update_image_cleanup()
        self.obIngester.set_ecat_toggles_cleanup()
        self.obIngester.set_htme_toggles_cleanup()
        self.obIngester.set_gsa_toggles_cleanup()
        self.obIngester.set_va_toggles_cleanup()

## end ##