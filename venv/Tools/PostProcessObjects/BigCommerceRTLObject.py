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
            print(row)
            if 'Filter' in row:
                if row['Filter'] == 'Fail' and not self.full_run:
                    self.obReporter.update_report('Alert','No change')
                    return True, df_collect_product_base_data
            else:
                self.obReporter.update_report('Alert','This product was processed anyway')
                # return False, df_collect_product_base_data

        # success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return False, df_collect_product_base_data

    def process_changes(self, df_collect_product_base_data):
        
        for colName, row in df_collect_product_base_data.iterrows():
            product_id = row['ProductId']
            price_id = row['ProductPriceId']
            base_id = row['BaseProductPriceId']
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
            if 'ECATOnContract' in row:
                ecat_contract = row['ECATOnContract']
            if 'ECATPricingApproved' in row:
                ecat_approved = row['ECATPricingApproved']
                
            htme_contract = -1
            htme_approved = -1
            if 'HTMETOnContract' in row:
                htme_contract = row['HTMETOnContract']
            if 'HTMEPricingApproved' in row:
                htme_approved = row['HTMEPricingApproved']
                
            gsa_contract = -1
            gsa_approved = -1
            if 'GSAOnContract' in row:
                gsa_contract = row['GSAOnContract']
            if 'GSAPricingApproved' in row:
                gsa_approved = row['GSAPricingApproved']
                
            va_contract = -1
            va_approved = -1
            if 'VAOnContract' in row:
                va_contract = row['VAOnContract']
            if 'VAPricingApproved' in row:
                va_approved = row['VAPricingApproved']
    

        self.obIngester.set_toggles(product_id, price_id, base_id, fy_product_number, vendor_part_number, price_toggle,
                                    data_toggle, is_discontinued, allow_purchases, is_visible, update_image,
                                    ecat_contract, ecat_approved, htme_contract, htme_approved,
                                    gsa_contract, gsa_approved, va_contract, va_approved)

        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.set_toggles_cleanup()



## end ##