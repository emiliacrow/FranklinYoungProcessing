



def test_cap_wrap(obDal,test_dt):
    get_id = obDal.unit_of_issue_cap(6, 1, 3)
    print(get_id)


def test_later(obDal, test_dt):
    get_id = obDal.image_size_cap('file path 6',600,400,1)
    print(get_id)
    get_id = obDal.image_cap('image DAL url', 0, 3, 0, 'image 4', 'file path DAL', 600, 400)
    print(get_id)
    get_id = obDal.image_cap('video DAL url', 1, 3, 0)
    print(get_id)



def tested_caps(obDal,test_dt):
    get_id = obDal.product_promo_cap(35.5,test_dt,test_dt,15,.15,30,3)
    print(get_id)
    get_id = obDal.unit_of_issue_symbol_cap('DL', 'DAL', 'DAL')
    print(get_id)
    get_id = obDal.unit_of_issue_cap(6, 1, 3)
    print(get_id)


def test_odds(obDal):
    get_id = obDal.lead_time_cap(5, 3)
    print(get_id)
    get_id = obDal.fsc_code_cap('fsc-dal-test', 'test')
    print(get_id)
    get_id = obDal.hazardous_code_cap('DAL-hazard', 'DALerium', '')
    print(get_id)
    get_id = obDal.product_document_cap(5, '', '', '', 'a url')
    print(get_id)
    get_id = obDal.product_seo_cap('DAL keywords')
    print(get_id)
    get_id = obDal.product_warranty_cap('DAL-warranty','')
    print(get_id)
    get_id = obDal.shipping_instructions_cap('DAL-shipment','',0,0)
    print(get_id)
    get_id = obDal.species_cap('DAL-human','')
    print(get_id)
    get_id = obDal.tube_cap('Dal tube','tube','tube')
    print(get_id)
    get_id = obDal.naics_code_cap('DAL-naics-1', '')
    print(get_id)
    get_id = obDal.national_drug_code_cap('DAL-NDC-1', '')
    print(get_id)


def test_ven_man(obDal):
    # feeds attribute table
    get_id = obDal.country_cap('New Manford City', 'MC', 'NMC')
    print(get_id)
    get_id = obDal.manufacturer_cap('New Manufacturer', 1)
    print(get_id)
    # tests lowest level insert
    print(get_id)


def test_attributes(obDal):
    table_list = ['Component','RecommendedStorage','Accuracy','Amperage','ApertureSize','ApparelSize','Capacity','Color','Depth','Diameter','Dimensions','ExteriorDimensions','Height','InnerDiameter','InteriorDimensions','Mass','Material','OuterDiameter','ParticleSize','PoreSize','Precision','Speed','Sterility','SurfaceTreatment','TankCapacity','TemperatureRange','Thickness','Tolerance','Voltage','Wattage','Wavelength','WeightRange','Width']

    for each_table in table_list:
        att_desc = 'att-desc-test: '+each_table
        get_id = obDal.attribute_cap(att_desc, each_table)
        print(get_id, att_desc)


def test_component(obDal):
    print('components')
    get_id = obDal.component_set_cap(1, None, None, 2, None)
    print(get_id)
    get_id = obDal.component_set_cap(1, None, None, None, 'a knob')
    print(get_id)
    get_id = obDal.component_set_cap(1, None, None, None, 'a stick')
    print(get_id)
    get_id = obDal.component_set_cap(None, None, 'a kite', None, 'a stick')
    print(get_id)
    print('set 2')
    get_id = obDal.component_set_cap(None, 'COMP-1', None, None, 'a component')
    print(get_id)
    get_id = obDal.component_set_cap(None, 'COMP-1', 'a comp set', None, 'a component')
    print(get_id)
    get_id = obDal.component_set_cap(None, None, 'a comp set', None, 'a new component')
    print(get_id)
    get_id = obDal.component_set_cap(None, 'COMP-1', None, None, 'component 3')
    print(get_id)
    print('set 3')
    get_id = obDal.component_set_cap(1, None, None, None, 'component 4')
    print(get_id)
    get_id = obDal.component_set_cap(None, 'COMP-2', 'a second comp set', None, 'component 3')
    print(get_id)
    get_id = obDal.component_set_cap(None, 'COMP-2', None, 2, None)
    print(get_id)


def test_prices(obDal,test_dt):
    get_id = obDal.va_product_price_cap(1,test_dt,test_dt,'E-113','E-113-1',.15,.13,12,'VA-SIN',35.5)
    print(get_id)
    get_id = obDal.htme_product_price_cap(1,test_dt,test_dt,35.5,'F-113','F-113-1',.15,.13,55.02)
    print(get_id)
    get_id = obDal.gsa_product_price_cap(1,test_dt,test_dt,19.5,'E-113','E-113-1',.35,.25,23.5,'SIN','bad',20.1)
    print(get_id)
    get_id = obDal.ecat_product_price_cap(1,test_dt,test_dt,35.5,'F-113','F-113-1',.15,.13,12,55.02)
    print(get_id)
    get_id = obDal.fedmall_product_price_cap(1,test_dt,test_dt,35.5,'F-113','F-113-1',.15,.13,55.02)
    print(get_id)
    get_id = obDal.oconus_product_cap(55.02,'DAL-cost',3,3)
    print(get_id)
    get_id = obDal.product_promo_cap(35.5,test_dt,test_dt,15,.15,30,3)
    print(get_id)
    get_id = obDal.product_promo_cap(35.5,test_dt,test_dt,15,.15,30,8)
    print(get_id)

