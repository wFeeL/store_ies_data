import csv 
import json 
import os 
import ips 
GAME_TICKS =100 
FORECAST_DIR =os .path .expanduser ('~/ips3-sandbox')
DEFAULT_FORECAST_FILE =os .path .join (FORECAST_DIR ,'forecast.csv')
MI ={'good_fill_ratio':0.88 ,'weak_fill_ratio':0.72 ,'bad_fill_ratio':0.58 ,'preferred_ask_high':9.2 ,'poor_execution_ratio':0.35 ,'good_execution_ratio':0.72 ,'high_volume_soft_cap':8.0 ,'market_log_window':12 ,'market_log_ewma_alpha':0.34 ,'near_zero_fill_ratio':0.12 ,'near_zero_execution_ratio':0.12 ,'overpriced_gap_steps':1.1 ,'underpriced_gap_steps':1.6 ,'competition_window':14 ,'competition_buy_pressure':0.2 ,'competition_sell_pressure':-0.22 }
PI ={'s1':{'strength':1.0 ,'storm_risk':0.0 },'s7':{'strength':1.02 ,'storm_risk':0.0 },'s8':{'strength':0.95 ,'storm_risk':0.0 },'a3':{'strength':0.96 ,'storm_risk':0.16 },'a5':{'strength':1.0 ,'storm_risk':0.38 },'a6':{'strength':0.98 ,'storm_risk':0.24 },'a8':{'strength':0.97 ,'storm_risk':0.22 }}
STATE_FILE =os .path .expanduser ('~/ips3-sandbox/ies_state.json')
MOV =0.25 
MIN_RESERVE =0.8 
PMN =2.0 
PMX =20.0 
SOC_FLOOR_FRAC =0.06 
SOC_CEIL_FRAC =0.95 
ENDGAME_TICKS =5 
LOOKAHEAD =max (8 ,GAME_TICKS //12 )
MARKET_HISTORY_WINDOW =8 
STRICT_FIRST_TICK_CAP =True 
SOLAR_SEED_FACTORS ={'s1':0.5 ,'s7':0.62 ,'s8':0.47 }
WIND_SEED_FACTORS ={'a3':0.0046 ,'a5':0.005 ,'a6':0.0048 ,'a8':0.0047 }
LOAD_BIAS_PRIOR =0.6 
LOAD_TYPE_BIAS_PRIORS ={'factory':0.48 ,'houseA':0.46 ,'houseB':0.52 ,'office':0.57 ,'hospital':0.62 }
LOAD_TYPE_BIAS_BOUNDS ={'factory':(0.26 ,1.18 ),'houseA':(0.24 ,1.18 ),'houseB':(0.26 ,1.14 ),'office':(0.28 ,1.18 ),'hospital':(0.36 ,1.22 )}
LOSS_MODEL_PRIOR ={'gen_quad':0.0058 ,'gen_lin':0.212 ,'load_quad':0.0103 ,'load_lin':-0.0687 ,'base':0.07 }
FORECAST_INDEX_TO_NAME ={0 :'hospital',1 :'factory',2 :'office',3 :'houseA',4 :'houseB',5 :'sun',6 :'wind'}
OTF ={'hospital':'hospital','factory':'factory','office':'office','houseA':'houseA','houseB':'houseB'}
FORECAST_HEADER_TO_KEY ={'ветер':'wind','солнце':'sun','больницы':'hospital','заводы':'factory','офисы':'office','дома а':'houseA','housea':'houseA','дома б':'houseB','houseb':'houseB','hospital':'hospital','factory':'factory','office':'office','sun':'sun','wind':'wind'}
FSO =('hospital','factory','office','houseA','houseB','sun','wind')
_FORECAST_CACHE ={'key':None ,'payload':None }

def cl (x ,lo ,hi ):
	return max (lo ,min (hi ,x ))

def round_vol (x ):
	return round (max (0.0 ,float (x )),3 )

def round_price (x ,price_max =20.0 ,price_step =0.2 ):
	step =max (0.01 ,float (price_step ))
	hi =cl (float (price_max ),PMN ,PMX )
	x =cl (float (x ),PMN ,hi )
	return round (round (x /step )*step ,2 )

def sf (value ,default =0.0 ):
	try :
		if value is None :
			return None if default is None else float (default )
		return float (value )
	except Exception :
		return None if default is None else float (default )

def si (value ,default =0 ):
	try :
		if value is None :
			return int (default )
		return int (value )
	except Exception :
		return int (default )

def avg (values ,default =0.0 ):
	if not values :
		return float (default )
	return sum ((float (v )for v in values ))/float (len (values ))

def addr_to_str (address ):
	if isinstance (address ,list ):
		return '|'.join ((str (x )for x in address ))
	if isinstance (address ,tuple ):
		return '|'.join ((str (x )for x in address ))
	return str (address )

def normalize_forecast_header (name ):
	s =str (name or '').strip ().lower ().replace ('ё','е').replace ('_',' ').replace ('-',' ')
	s =' '.join (s .split ())
	return FORECAST_HEADER_TO_KEY .get (s ,'')

def _parse_forecast_csv (path ):
	series ={name :[]for name in FSO }
	rows_read =0 
	try :
		with open (path ,'r',encoding ='utf-8-sig',newline ='')as f :
			reader =csv .DictReader (f )
			if not reader .fieldnames :
				return None 
			header_map ={field :normalize_forecast_header (field )for field in reader .fieldnames }
			recognized ={canon for canon in header_map .values ()if canon }
			if not {'wind','sun'}.issubset (recognized ):
				return None 
			for row in reader :
				if not any ((str (v ).strip ()for v in row .values ())):
					continue 
				rows_read +=1 
				normalized ={}
				for raw_key ,value in row .items ():
					canon =header_map .get (raw_key ,'')
					if canon :
						normalized [canon ]=sf (value ,0.0 )
				for name in FSO :
					if name in normalized :
						series [name ].append (normalized [name ])
	except Exception :
		return None 
	max_len =max ((len (v )for v in series .values ()),default =0 )
	if max_len <=0 :
		return None 
	return {'path':path ,'rows':rows_read ,'series_lengths':{name :len (values )for name ,values in series .items ()if values },'bundle':{name :{'data':list (values ),'spread':None ,'source':'csv'}for name ,values in series .items ()if values }}

def find_external_forecast_file ():
	preferred =[DEFAULT_FORECAST_FILE ]
	for path in preferred :
		path =os .path .abspath (os .path .expanduser (path ))
		if _parse_forecast_csv (path )is not None :
			return path 
	try :
		candidates =[]
		for entry in os .scandir (FORECAST_DIR ):
			if not entry .is_file ():
				continue 
			if not entry .name .lower ().startswith ('forecast')or not entry .name .lower ().endswith ('.csv'):
				continue 
			candidates .append ((entry .stat ().st_mtime_ns ,entry .path ))
		for _ ,path in sorted (candidates ,reverse =True ):
			if _parse_forecast_csv (path )is not None :
				return path 
	except Exception :
		return None 
	return None 

def load_external_forecast_csv ():
	path =find_external_forecast_file ()
	if not path :
		return None 
	try :
		stat =os .stat (path )
		cache_key =(path ,stat .st_mtime_ns ,stat .st_size )
	except Exception :
		cache_key =(path ,None ,None )
	if _FORECAST_CACHE .get ('key')==cache_key :
		return _FORECAST_CACHE .get ('payload')
	payload =_parse_forecast_csv (path )
	_FORECAST_CACHE ['key']=cache_key 
	_FORECAST_CACHE ['payload']=payload 
	return payload 

def corridor_fallback_value (cfg ,name ):
	cfg =cfg or {}
	corridor_map ={'sun':'corridorSun','wind':'corridorWind','hospital':'corridorHospital','factory':'corridorFactory','office':'corridorOffice','houseA':'corridorHouseA','houseB':'corridorHouseB'}
	default_map ={'sun':0.5 ,'wind':0.5 ,'hospital':0.25 ,'factory':0.5 ,'office':0.5 ,'houseA':0.5 ,'houseB':0.5 }
	key =corridor_map .get (name )
	default =default_map .get (name ,0.5 )
	return sf (cfg .get (key ,default ),default )if key else default 

def forecast_required_series (object_rows =None ):
	required =['sun','wind']
	if not object_rows :
		return required 
	for row in object_rows :
		fc_name =OTF .get (row .get ('type'))
		if fc_name and fc_name not in required :
			required .append (fc_name )
	return required 

def forecast_validated_rows (bundle ,game_length ):
	meta =bundle .get ('_meta',{})
	validated =si (meta .get ('rows',0 ),0 )
	if validated >0 :
		return validated 
	required_series =meta .get ('required_series',['sun','wind'])
	lengths =[si (meta .get ('series_lengths',{}).get (name ,0 ),0 )for name in required_series ]
	lengths =[length for length in lengths if length >0 ]
	if lengths :
		return min (max (GAME_TICKS ,game_length ),min (lengths ))
	return 0 

def forecast_has_valid_tick (bundle ,tick ):
	if tick <0 :
		return False 
	meta =bundle .get ('_meta',{})
	required_rows =si (meta .get ('required_rows',GAME_TICKS ),GAME_TICKS )
	return tick <forecast_validated_rows (bundle ,required_rows )

def _extract_forecast_series (item ):
	if item is None :
		return ([],None )
	if isinstance (item ,dict ):
		if 'data'in item :
			return ([sf (v ,0.0 )for v in item .get ('data',[])],sf (item .get ('spread',None ),None ))
		if 'forecast'in item :
			forecast =item .get ('forecast',{})
			return ([sf (v ,0.0 )for v in forecast .get ('values',[])],sf (item .get ('spread',None ),None ))
	if isinstance (item ,(list ,tuple )):
		spread =sf (getattr (item ,'spread',None ),None )
		return ([sf (v ,0.0 )for v in item ],spread )
	try :
		spread =sf (getattr (item ,'spread',None ),None )
		return ([sf (v ,0.0 )for v in item ],spread )
	except Exception :
		return ([],None )

def load_native_forecast_bundle (psm ):
	raw =psm .get ('forecasts')if isinstance (psm ,dict )else getattr (psm ,'forecasts',None )
	out ={}
	if isinstance (raw ,list ):
		for idx ,item in enumerate (raw ):
			name =FORECAST_INDEX_TO_NAME .get (idx ,f'f{idx }')
			data ,spread =_extract_forecast_series (item )
			if data :
				out [name ]={'data':data ,'spread':spread ,'source':'psm'}
	elif isinstance (raw ,dict ):
		for name in FSO :
			data ,spread =_extract_forecast_series (raw .get (name ))
			if data :
				out [name ]={'data':data ,'spread':spread ,'source':'psm'}
	else :
		for name in FSO :
			seq =getattr (raw ,name ,None )if raw is not None else None 
			data ,spread =_extract_forecast_series (seq )
			if data :
				out [name ]={'data':data ,'spread':spread ,'source':'psm'}
	out ['_meta']={'source':'psm','path':None ,'rows':max ((len (v .get ('data',[]))for v in out .values ()if isinstance (v ,dict )and 'data'in v ),default =0 ),'series_lengths':{name :len (item .get ('data',[]))for name ,item in out .items ()if isinstance (item ,dict )and 'data'in item }}
	return out 

def harmonize_forecast_bundle (external ,native ,required_rows ,cfg =None ,object_rows =None ):
	cfg =cfg or {}
	required_rows =max (GAME_TICKS ,si (required_rows ,GAME_TICKS ))
	required_series =forecast_required_series (object_rows )
	warnings =[]
	fallbacks =[]
	series_lengths ={}
	out ={}
	ext_bundle =(external or {}).get ('bundle',{})
	native_bundle =native or {}
	for name in FSO :
		ext_item =ext_bundle .get (name )
		native_item =native_bundle .get (name )
		data =[]
		if ext_item :
			data =list (ext_item .get ('data',[]))[:required_rows ]
		source ='csv'if ext_item else 'psm'
		if len (data )<required_rows and native_item :
			tail =list (native_item .get ('data',[]))[len (data ):required_rows ]
			if tail :
				data .extend (tail )
				fallbacks .append (f'{name }:tail_from_psm')
		spread =sf (ext_item .get ('spread',None ),None )if ext_item else None 
		if spread is None and native_item :
			native_spread =sf (native_item .get ('spread',None ),None )
			if native_spread is not None :
				spread =native_spread 
				if ext_item :
					fallbacks .append (f'{name }:spread_from_psm')
		if spread is None :
			spread =corridor_fallback_value (cfg ,name )
			fallbacks .append (f'{name }:spread_from_config')
		if data :
			out [name ]={'data':data ,'spread':spread ,'source':source }
			series_lengths [name ]=len (data )
		elif name in required_series :
			warnings .append (f'missing_series:{name }')
	available_lengths =[series_lengths .get (name ,0 )for name in required_series if series_lengths .get (name ,0 )>0 ]
	validated_rows =min (required_rows ,min (available_lengths ))if available_lengths else 0 
	for name in required_series :
		series_len =series_lengths .get (name ,0 )
		if 0 <series_len <required_rows :
			warnings .append (f'short_series:{name }:{series_len }/{required_rows }')
	if validated_rows <required_rows :
		warnings .append (f'forecast_horizon_short:{validated_rows }/{required_rows }')
	source ='csv'if external else 'psm'
	out ['_meta']={'source':source ,'path':external .get ('path')if external else None ,'rows':validated_rows ,'required_rows':required_rows ,'required_series':required_series ,'series_lengths':series_lengths ,'fallbacks':sorted (set (fallbacks )),'warnings':sorted (set (warnings ))}
	return out 

def is_compact_object (obj ):
	return isinstance (obj ,list )

def get_tick (psm ):
	if isinstance (psm ,dict ):
		return si (psm .get ('tick',0 ))
	return si (getattr (psm ,'tick',0 ))

def get_game_length (psm ):
	if isinstance (psm ,dict ):
		value =si (psm .get ('gameLength',GAME_TICKS ),GAME_TICKS )
	else :
		value =si (getattr (psm ,'gameLength',GAME_TICKS ),GAME_TICKS )
	return value if value >0 else GAME_TICKS 

def get_total_power_tuple (psm ):
	raw =psm .get ('total_power')if isinstance (psm ,dict )else getattr (psm ,'total_power',None )
	if isinstance (raw ,list ):
		return (sf (raw [0 ],0.0 ),sf (raw [1 ],0.0 ),sf (raw [2 ],0.0 ),sf (raw [3 ],0.0 ))
	return (sf (getattr (raw ,'generated',0.0 ),0.0 ),sf (getattr (raw ,'consumed',0.0 ),0.0 ),sf (getattr (raw ,'external',0.0 ),0.0 ),sf (getattr (raw ,'losses',0.0 ),0.0 ))

def get_weather_now (psm ,name ):
	raw =psm .get (name )if isinstance (psm ,dict )else getattr (psm ,name ,None )
	if isinstance (raw ,list ):
		return sf (raw [0 ],0.0 )
	return sf (getattr (raw ,'now',0.0 ),0.0 )

def get_forecast_bundle (psm ,game_length =GAME_TICKS ,cfg =None ,object_rows =None ):
	ext =load_external_forecast_csv ()
	native =load_native_forecast_bundle (psm )
	return harmonize_forecast_bundle (ext ,native ,max (GAME_TICKS ,game_length ),cfg =cfg ,object_rows =object_rows )

def get_object_list (psm ):
	if isinstance (psm ,dict ):
		return list (psm .get ('objects',[]))
	return list (getattr (psm ,'objects',[]))

def get_network_items (psm ):
	raw =psm .get ('networks')if isinstance (psm ,dict )else getattr (psm ,'networks',{})
	return list (raw .items ())

def get_exchange_list (psm ):
	if isinstance (psm ,dict ):
		return list (psm .get ('exchange',[]))
	return list (getattr (psm ,'exchange',[]))

def get_exchange_log (psm ):
	if isinstance (psm ,dict ):
		return list (psm .get ('exchangeLog',[]))
	return list (getattr (psm ,'exchangeLog',[]))

def get_config_dict (psm ):
	if isinstance (psm ,dict ):
		return dict (psm .get ('config',{}))
	cfg =getattr (psm ,'config',None )
	if isinstance (cfg ,dict ):
		return dict (cfg )
	out ={}
	if cfg is None :
		return out 
	for key in dir (cfg ):
		if key .startswith ('_'):
			continue 
		try :
			value =getattr (cfg ,key )
		except Exception :
			continue 
		if callable (value ):
			continue 
		out [key ]=value 
	return out 

def obj_id (obj ):
	if is_compact_object (obj ):
		return obj [0 ]
	return getattr (obj ,'id',None )

def obj_type (obj ):
	if is_compact_object (obj ):
		return str (obj [1 ])
	return str (getattr (obj ,'type',''))

def obj_contract (obj ):
	if is_compact_object (obj ):
		return sf (obj [2 ],0.0 )
	return sf (getattr (obj ,'contract',0.0 ),0.0 )

def obj_address (obj ):
	if is_compact_object (obj ):
		return list (obj [3 ])
	return list (getattr (obj ,'address',[]))

def obj_address_key (obj ):
	return addr_to_str (obj_address (obj ))

def obj_path (obj ):
	if is_compact_object (obj ):
		return obj [4 ]
	return getattr (obj ,'path',[])

def obj_score_now (obj ):
	if is_compact_object (obj ):
		raw =obj [7 ][0 ]if len (obj )>7 and obj [7 ]else [0.0 ,0.0 ]
		return (sf (raw [0 ],0.0 ),sf (raw [1 ],0.0 ))
	score_now =getattr (getattr (obj ,'score',None ),'now',None )
	return (sf (getattr (score_now ,'income',0.0 ),0.0 ),sf (getattr (score_now ,'loss',0.0 ),0.0 ))

def obj_power_now (obj ):
	if is_compact_object (obj ):
		raw =obj [8 ][0 ]if len (obj )>8 and obj [8 ]else [0.0 ,0.0 ]
		return (sf (raw [0 ],0.0 ),sf (raw [1 ],0.0 ))
	power_now =getattr (getattr (obj ,'power',None ),'now',None )
	return (sf (getattr (power_now ,'generated',0.0 ),0.0 ),sf (getattr (power_now ,'consumed',0.0 ),0.0 ))

def obj_charge_now (obj ):
	if is_compact_object (obj ):
		if len (obj )>6 and isinstance (obj [6 ],list )and obj [6 ]:
			return sf (obj [6 ][0 ],0.0 )
		return None 
	ch =getattr (obj ,'charge',None )
	if ch is None :
		return None 
	return sf (getattr (ch ,'now',0.0 ),0.0 )

def obj_wind_rotation_now (obj ):
	if is_compact_object (obj ):
		if len (obj )>9 and isinstance (obj [9 ],list )and obj [9 ]:
			return sf (obj [9 ][0 ],0.0 )
		return None 
	wr =getattr (obj ,'windRotation',None )
	if wr is None :
		return None 
	return sf (getattr (wr ,'now',0.0 ),0.0 )

def obj_failed (obj ):
	if is_compact_object (obj ):
		if len (obj )>5 :
			return si (obj [5 ],0 )
		return 0 
	return si (getattr (obj ,'failed',0 ),0 )

def net_location (net ):
	if isinstance (net ,list ):
		return net [0 ]
	return getattr (net ,'location',[])

def net_upflow (net ):
	if isinstance (net ,list ):
		return sf (net [1 ],0.0 )
	return sf (getattr (net ,'upflow',0.0 ),0.0 )

def net_downflow (net ):
	if isinstance (net ,list ):
		return sf (net [2 ],0.0 )
	return sf (getattr (net ,'downflow',0.0 ),0.0 )

def net_losses (net ):
	if isinstance (net ,list ):
		return sf (net [3 ],0.0 )
	return sf (getattr (net ,'losses',0.0 ),0.0 )

def exchange_receipt_data (receipt ):
	if isinstance (receipt ,list ):
		asked =sf (receipt [0 ],0.0 )
		out ={'askedAmount':asked ,'askedPrice':sf (receipt [1 ],0.0 ),'contractedAmount':sf (receipt [2 ],0.0 ),'contractedPrice':sf (receipt [3 ],0.0 ),'instantAmount':sf (receipt [4 ],0.0 )}
	else :
		asked =sf (getattr (receipt ,'askedAmount',0.0 ),0.0 )
		out ={'askedAmount':asked ,'askedPrice':sf (getattr (receipt ,'askedPrice',0.0 ),0.0 ),'contractedAmount':sf (getattr (receipt ,'contractedAmount',0.0 ),0.0 ),'contractedPrice':sf (getattr (receipt ,'contractedPrice',0.0 ),0.0 ),'instantAmount':sf (getattr (receipt ,'instantAmount',0.0 ),0.0 )}
	out ['side']='buy'if asked >0 else 'sell'if asked <0 else 'flat'
	return out 

def default_state ():
	return {'prev_useful_supply_est':None ,'prev_useful_energy_actual':None ,'last_sell_volume':0.0 ,'abs_err_ewma':1.2 ,'loss_ratio_ewma':0.18 ,'fill_ratio_ewma':0.84 ,'execution_ratio_ewma':0.72 ,'market_ref':4.8 ,'exchange_price_history':[],'market_history':[],'competition_history':[],'sell_bias_steps':0.0 ,'load_bias_total':LOAD_BIAS_PRIOR ,'load_abs_err':2.0 ,'startup_load_scale':1.0 ,'startup_mode_until':-1 ,'startup_last_ratio':1.0 ,'startup_last_update_tick':-1 ,'load_mix':{'counts':{},'houseb_share':0.0 },'object_models':{},'storage_mode':'hold','weather_history':{'wind':[],'sun':[]},'loss_model':dict (LOSS_MODEL_PRIOR ,scale =1.0 )}

def load_state ():
	try :
		with open (STATE_FILE ,'r',encoding ='utf-8')as f :
			data =json .load (f )
		if isinstance (data ,dict ):
			data .pop ('forecast_profile',None )
			st =default_state ()
			st .update (data )
			return st 
	except Exception :
		pass 
	return default_state ()

def save_state (state ):
	try :
		with open (STATE_FILE ,'w',encoding ='utf-8')as f :
			json .dump (state ,f ,ensure_ascii =False ,indent =2 )
	except Exception :
		pass 

def _model_key (address ,kind ):
	return f'{kind }:{address }'

def get_model (state ,key ,kind ):
	models =state .setdefault ('object_models',{})
	mkey =_model_key (key ,kind )
	if mkey not in models :
		if kind =='solar':
			prior =PI .get (key ,{})
			models [mkey ]={'kind':kind ,'factor':SOLAR_SEED_FACTORS .get (key ,0.65 ),'err':0.8 ,'samples':0 ,'strength_bias':sf (prior .get ('strength',1.0 ),1.0 )}
		elif kind =='wind':
			prior =PI .get (key ,{})
			models [mkey ]={'kind':kind ,'factor':WIND_SEED_FACTORS .get (key ,0.005 ),'rot_factor':80.0 ,'wind_to_rot':0.04 ,'rot_curve':{},'max_power_seen':0.0 ,'err':2.5 ,'last_failed':0 ,'samples':0 ,'storm_risk':sf (prior .get ('storm_risk',0.0 ),0.0 ),'strength_bias':sf (prior .get ('strength',1.0 ),1.0 )}
		else :
			models [mkey ]={'kind':kind ,'bias':None ,'err':0.6 ,'samples':0 }
	return models [mkey ]

def update_wind_rot_curve (model ,rotation_now ,actual_power ):
	if rotation_now <=0.03 or actual_power <0.0 :
		return 
	curve =model .setdefault ('rot_curve',{})
	bucket =round (rotation_now /0.05 )*0.05 
	key =f'{bucket :.2f}'
	prev =sf (curve .get (key ,0.0 ),0.0 )
	curve [key ]=max (prev ,actual_power )
	if len (curve )>80 :
		keys =sorted (curve .keys (),key =lambda k :float (k ))
		for old in keys [:-80 ]:
			curve .pop (old ,None )

def estimate_wind_from_curve (model ,rotation ):
	curve =model .get ('rot_curve')or {}
	if rotation <=0.03 or len (curve )<3 :
		return None 
	pts =sorted (((sf (k ,0.0 ),sf (v ,0.0 ))for k ,v in curve .items ()))
	close =[(r ,p )for r ,p in pts if abs (r -rotation )<=0.18 ]
	if not close :
		close =sorted (pts ,key =lambda rp :abs (rp [0 ]-rotation ))[:4 ]
	if not close :
		return None 
	num =0.0 
	den =0.0 
	near_max =0.0 
	for r ,p in close :
		d =abs (r -rotation )
		w =1.0 /max (d ,0.03 )
		num +=w *p 
		den +=w 
		if d <=0.08 :
			near_max =max (near_max ,p )
	est =num /max (den ,1e-09 )
	if near_max >0.0 :
		est =max (est ,0.92 *near_max )
	return max (0.0 ,est )

def extract_object_rows (psm ):
	rows =[]
	for obj in get_object_list (psm ):
		generated ,consumed =obj_power_now (obj )
		income ,loss =obj_score_now (obj )
		rows .append ({'id':str (obj_id (obj )),'type':obj_type (obj ),'contract':obj_contract (obj ),'address':obj_address_key (obj ),'path':json .dumps (obj_path (obj ),ensure_ascii =False ),'generated':generated ,'consumed':consumed ,'income':income ,'loss':loss ,'charge_now':obj_charge_now (obj ),'wind_rotation':obj_wind_rotation_now (obj ),'failed':obj_failed (obj )})
	return rows 

def extract_network_rows (psm ):
	rows =[]
	for idx ,net in get_network_items (psm ):
		rows .append ({'network_index':idx ,'location':json .dumps (net_location (net ),ensure_ascii =False ),'upflow':net_upflow (net ),'downflow':net_downflow (net ),'losses':net_losses (net )})
	return rows 

def aggregate_objects (rows ):
	info ={'gen_total':0.0 ,'cons_total':0.0 ,'income_total':0.0 ,'loss_total':0.0 ,'by_type':{},'storages':[]}
	for row in rows :
		typ =row ['type']
		bt =info ['by_type'].setdefault (typ ,{'count':0 ,'generated':0.0 ,'consumed':0.0 ,'income':0.0 ,'loss':0.0 })
		bt ['count']+=1 
		bt ['generated']+=row ['generated']
		bt ['consumed']+=row ['consumed']
		bt ['income']+=row ['income']
		bt ['loss']+=row ['loss']
		info ['gen_total']+=row ['generated']
		info ['cons_total']+=row ['consumed']
		info ['income_total']+=row ['income']
		info ['loss_total']+=row ['loss']
		if row ['type']=='storage':
			info ['storages'].append ({'id':row ['address'].split ('|')[0 ],'soc':sf (row ['charge_now'],0.0 )})
	return info 

def aggregate_networks (rows ):
	return {'upflow_total':sum ((r ['upflow']for r in rows )),'downflow_total':sum ((r ['downflow']for r in rows )),'losses_total':sum ((r ['losses']for r in rows ))}

def count_forecast_objects (object_rows ):
	counts ={k :0 for k in OTF }
	for row in object_rows :
		typ =row .get ('type')
		if typ in counts :
			counts [typ ]+=1 
	return counts 

def aggregate_forecast_load (bundle ,object_rows ,tick ):
	counts =count_forecast_objects (object_rows )
	total =0.0 
	for typ ,fc_name in OTF .items ():
		total +=counts .get (typ ,0 )*get_forecast_value (bundle ,fc_name ,tick )
	return total 

def predict_total_losses (state ,total_gen ,total_load ):
	model =state .setdefault ('loss_model',dict (LOSS_MODEL_PRIOR ))
	gq =sf (model .get ('gen_quad',LOSS_MODEL_PRIOR ['gen_quad']),LOSS_MODEL_PRIOR ['gen_quad'])
	gl =sf (model .get ('gen_lin',LOSS_MODEL_PRIOR ['gen_lin']),LOSS_MODEL_PRIOR ['gen_lin'])
	lq =sf (model .get ('load_quad',LOSS_MODEL_PRIOR ['load_quad']),LOSS_MODEL_PRIOR ['load_quad'])
	ll =sf (model .get ('load_lin',LOSS_MODEL_PRIOR ['load_lin']),LOSS_MODEL_PRIOR ['load_lin'])
	base =sf (model .get ('base',LOSS_MODEL_PRIOR ['base']),LOSS_MODEL_PRIOR ['base'])
	scale =sf (model .get ('scale',1.0 ),1.0 )
	pred =gq *total_gen *total_gen +gl *total_gen +lq *total_load *total_load +ll *total_load +base 
	pred =max (0.0 ,pred )
	return pred *cl (scale ,0.6 ,1.6 )

def get_forecast_value (bundle ,name ,tick ):
	if not forecast_has_valid_tick (bundle ,tick ):
		return 0.0 
	item =bundle .get (name )
	if not item :
		return 0.0 
	data =item .get ('data',[])
	if tick <0 or tick >=len (data ):
		return 0.0 
	return sf (data [tick ],0.0 )

def get_forecast_spread (bundle ,name ,fallback =0.0 ):
	item =bundle .get (name )
	if not item :
		return fallback 
	return sf (item .get ('spread',fallback ),fallback )

def get_type_load_prior (state ,obj_type ):
	prior =sf (LOAD_TYPE_BIAS_PRIORS .get (obj_type ,LOAD_BIAS_PRIOR ),LOAD_BIAS_PRIOR )
	mix =state .get ('load_mix',{})
	houseb_share =sf (mix .get ('houseb_share',0.0 ),0.0 )
	if obj_type =='houseB'and houseb_share >0.25 :
		damp =cl (1.0 -0.55 *(houseb_share -0.25 ),0.74 ,1.0 )
		prior *=damp 
	return cl (prior ,0.2 ,1.2 )

def get_type_load_bounds (state ,obj_type ):
	lo ,hi =LOAD_TYPE_BIAS_BOUNDS .get (obj_type ,(0.2 ,1.2 ))
	mix =state .get ('load_mix',{})
	houseb_share =sf (mix .get ('houseb_share',0.0 ),0.0 )
	if obj_type =='houseB'and houseb_share >0.25 :
		over =cl ((houseb_share -0.25 )/0.35 ,0.0 ,1.0 )
		hi =min (hi ,1.12 -0.08 *over )
	return (cl (lo ,0.1 ,2.0 ),cl (max (lo ,hi ),lo ,2.0 ))

def clamp_storage_soc (value ,cell_capacity ):
	return cl (sf (value ,0.0 ),0.0 ,max (0.0 ,cell_capacity ))

def startup_active (state ,tick ):
	return 0 <=si (state .get ('startup_mode_until',-1 ),-1 )and tick <=si (state .get ('startup_mode_until',-1 ),-1 )

def startup_scale (state ,tick ):
	scale =cl (sf (state .get ('startup_load_scale',1.0 ),1.0 ),0.0 ,1.0 )
	return scale if startup_active (state ,tick )or scale <0.999 else 1.0 

def startup_bias_active (state ,tick ):
	return startup_active (state ,tick )or startup_scale (state ,tick )<0.995 

def blended_load_base_bias (state ,obj_type ,tick ,type_prior =None ):
	prior =get_type_load_prior (state ,obj_type )if type_prior is None else sf (type_prior ,get_type_load_prior (state ,obj_type ))
	total_bias =sf (state .get ('load_bias_total',LOAD_BIAS_PRIOR ),LOAD_BIAS_PRIOR )
	prior_weight =0.45 
	scale =startup_scale (state ,tick )
	if scale <0.999 :
		prior_weight *=0.25 +0.75 *scale 
	return cl ((1.0 -prior_weight )*total_bias +prior_weight *prior ,0.02 ,1.2 )

def effective_load_trust (state ,model ,obj_type ,tick ):
	samples =si (model .get ('samples',0 ),0 )
	trust =cl (0.1 +0.08 *samples ,0.14 ,0.72 )
	if obj_type =='houseB'and sf (state .get ('load_mix',{}).get ('houseb_share',0.0 ),0.0 )>0.35 :
		trust =min (trust ,0.48 )
	if model .get ('bias')is None :
		trust =min (trust ,0.18 )
	scale =startup_scale (state ,tick )
	if scale <0.999 and model .get ('bias')is not None :
		trust =max (trust ,0.24 +0.26 *(1.0 -scale ))
	return cl (trust ,0.14 ,0.72 )

def effective_load_bounds (state ,obj_type ,tick ):
	lo ,hi =get_type_load_bounds (state ,obj_type )
	scale =startup_scale (state ,tick )
	if scale <0.999 :
		lo =min (lo ,0.02 +0.18 *scale )
	return (lo ,hi )

def refresh_static_runtime_context (state ,object_rows ):
	load_counts =count_forecast_objects (object_rows )
	total_load_objects =max (1 ,sum (load_counts .values ()))
	state ['load_mix']={'counts':load_counts ,'total_objects':total_load_objects ,'houseb_share':load_counts .get ('houseB',0 )/float (total_load_objects )}

def apply_startup_observation (state ,object_rows ,bundle ,tick ,total_consumed =None ):
	if total_consumed is None :
		return 
	if si (state .get ('startup_last_update_tick',-1 ),-1 )==tick :
		return 
	total_fc_now =aggregate_forecast_load (bundle ,object_rows ,tick )
	prev_scale =cl (sf (state .get ('startup_load_scale',1.0 ),1.0 ),0.0 ,1.0 )
	if total_fc_now >1e-06 :
		observed_ratio =cl (total_consumed /max (total_fc_now ,1e-06 ),0.0 ,1.1 )
		state ['startup_last_ratio']=observed_ratio 
		if tick ==0 and observed_ratio <0.15 :
			state ['startup_mode_until']=max (si (state .get ('startup_mode_until',-1 ),-1 ),4 )
		elif tick ==1 and observed_ratio <0.3 :
			state ['startup_mode_until']=max (si (state .get ('startup_mode_until',-1 ),-1 ),4 )
		startup_now =startup_active (state ,tick )
		if startup_now :
			scale_target =cl (observed_ratio ,0.0 ,1.0 )
			if tick <=1 and observed_ratio <0.35 :
				scale =cl (0.18 *prev_scale +0.82 *scale_target ,0.0 ,1.0 )
				startup_cap =cl (0.14 +0.45 *observed_ratio ,0.14 ,0.45 )
				state ['startup_load_scale']=min (scale ,startup_cap )
			else :
				scale =cl (0.35 *prev_scale +0.65 *scale_target ,0.0 ,1.0 )
				state ['startup_load_scale']=min (scale ,prev_scale +0.25 )
			if observed_ratio >0.75 :
				state ['startup_mode_until']=tick -1 
		elif prev_scale <0.999 :
			scale_target =cl (observed_ratio ,0.0 ,1.0 )
			scale_alpha =0.28 if scale_target >=prev_scale else 0.38 
			state ['startup_load_scale']=cl ((1.0 -scale_alpha )*prev_scale +scale_alpha *scale_target ,0.0 ,1.0 )
		startup_bias_now =startup_bias_active (state ,tick )
		load_bias =total_consumed /max (total_fc_now ,1e-06 )
		load_bias =cl (load_bias ,0.02 if startup_bias_now else 0.28 ,1.1 )
		alpha =0.35 if startup_bias_now else 0.1 
		state ['load_bias_total']=(1.0 -alpha )*sf (state .get ('load_bias_total',LOAD_BIAS_PRIOR ),LOAD_BIAS_PRIOR )+alpha *load_bias 
		pred_total =sf (state .get ('load_bias_total',LOAD_BIAS_PRIOR ),LOAD_BIAS_PRIOR )*total_fc_now 
		state ['load_abs_err']=0.9 *sf (state .get ('load_abs_err',2.0 ),2.0 )+0.1 *abs (total_consumed -pred_total )
	elif prev_scale <0.999 and tick >si (state .get ('startup_mode_until',-1 ),-1 ):
		state ['startup_load_scale']=min (1.0 ,cl (prev_scale +0.18 ,0.0 ,1.0 ))
	state ['startup_last_update_tick']=tick 

def apply_post_tick_learning (state ,object_rows ,weather ,bundle ,tick ,total_consumed =None ,total_losses =None ,marketable_useful_now =None ,total_generated =None ):
	sun_now =max (0.0 ,weather ['sun'])
	wind_now =max (0.0 ,weather ['wind'])
	hist =state .setdefault ('weather_history',{'wind':[],'sun':[]})
	hist ['wind']=(hist .get ('wind')or [])[-12 :]+[wind_now ]
	hist ['sun']=(hist .get ('sun')or [])[-12 :]+[sun_now ]
	apply_startup_observation (state ,object_rows ,bundle ,tick ,total_consumed =total_consumed )
	if total_consumed is not None and total_losses is not None :
		pred_loss =predict_total_losses (state ,sum ((r ['generated']for r in object_rows )),total_consumed )
		if pred_loss >1e-06 :
			scale =total_losses /pred_loss 
			lm =state .setdefault ('loss_model',dict (LOSS_MODEL_PRIOR ))
			lm ['scale']=0.88 *sf (lm .get ('scale',1.0 ),1.0 )+0.12 *cl (scale ,0.55 ,1.8 )
	if total_generated is not None and total_losses is not None and (total_generated >1e-09 ):
		current_loss_ratio =cl (total_losses /total_generated ,0.0 ,0.8 )
		state ['loss_ratio_ewma']=0.88 *sf (state .get ('loss_ratio_ewma',0.18 ),0.18 )+0.12 *current_loss_ratio 
	if marketable_useful_now is not None :
		prev_useful_est =state .get ('prev_useful_supply_est')
		if prev_useful_est is not None :
			err =marketable_useful_now -sf (prev_useful_est ,0.0 )
			state ['abs_err_ewma']=0.84 *sf (state .get ('abs_err_ewma',1.2 ),1.2 )+0.16 *abs (err )
	for row in object_rows :
		key =row ['address']
		typ =row ['type']
		if typ =='solar':
			model =get_model (state ,key ,'solar')
			actual =row ['generated']
			if sun_now >0.05 and actual >=0.0 :
				est =actual /max (sun_now ,1e-06 )
				model ['factor']=0.9 *sf (model .get ('factor',0.65 ),0.65 )+0.1 *cl (est ,0.0 ,1.6 )
			pred =sf (model .get ('factor',0.65 ),0.65 )*sun_now 
			model ['err']=0.88 *sf (model .get ('err',0.8 ),0.8 )+0.12 *abs (actual -pred )
			model ['samples']=si (model .get ('samples',0 ),0 )+1 
		elif typ =='wind':
			model =get_model (state ,key ,'wind')
			actual =row ['generated']
			rotation_now =max (0.0 ,sf (row .get ('wind_rotation',0.0 ),0.0 ))
			failed_now =si (row .get ('failed',0 ),0 )
			model ['max_power_seen']=max (sf (model .get ('max_power_seen',0.0 ),0.0 ),actual )
			if wind_now >0.2 and actual >=0.0 :
				est =actual /max (wind_now **3 ,1e-06 )
				model ['factor']=0.94 *sf (model .get ('factor',0.0048 ),0.0048 )+0.06 *cl (est ,0.0 ,0.02 )
			if wind_now >0.2 and rotation_now >0.03 :
				ratio =rotation_now /max (wind_now ,1e-06 )
				model ['wind_to_rot']=0.94 *sf (model .get ('wind_to_rot',0.04 ),0.04 )+0.06 *cl (ratio ,0.012 ,0.09 )
			if rotation_now >0.05 and actual >=0.0 :
				est_rot =actual /max (rotation_now **3 ,1e-06 )
				model ['rot_factor']=0.92 *sf (model .get ('rot_factor',80.0 ),80.0 )+0.08 *cl (est_rot ,12.0 ,180.0 )
				update_wind_rot_curve (model ,rotation_now ,actual )
			pred_direct =sf (model .get ('factor',0.0048 ),0.0048 )*wind_now **3 
			pred_rot =sf (model .get ('rot_factor',80.0 ),80.0 )*rotation_now **3 
			pred_curve =estimate_wind_from_curve (model ,rotation_now )
			pred =0.55 *pred_direct +0.45 *pred_rot 
			if pred_curve is not None :
				pred =0.3 *pred_direct +0.25 *pred_rot +0.45 *pred_curve 
			if failed_now >0 :
				pred *=0.7 
			elif si (model .get ('last_failed',0 ),0 )>0 :
				pred *=0.86 
			model ['err']=0.9 *sf (model .get ('err',2.5 ),2.5 )+0.1 *abs (actual -pred )
			model ['last_failed']=failed_now 
			model ['samples']=si (model .get ('samples',0 ),0 )+1 
		elif typ in OTF :
			model =get_model (state ,key ,'load')
			actual =row ['consumed']
			fc_name =OTF .get (typ )
			fc_now =get_forecast_value (bundle ,fc_name ,tick )
			type_prior =get_type_load_prior (state ,typ )
			lo ,hi =effective_load_bounds (state ,typ ,tick )
			base_bias =cl (blended_load_base_bias (state ,typ ,tick ,type_prior =type_prior ),lo ,hi )
			model_bias =sf (model .get ('bias',base_bias ),base_bias )
			adapt =0.08 
			scale =startup_scale (state ,tick )
			if scale <0.999 :
				adapt =cl (0.22 +0.45 *(1.0 -scale ),0.22 ,0.65 )
			if fc_now >0.05 and actual >=0.0 :
				est_bias =actual /max (fc_now ,1e-06 )
				target_bias =cl (est_bias ,lo ,hi )
				model ['bias']=(1.0 -adapt )*model_bias +adapt *target_bias 
			else :
				model ['bias']=0.97 *model_bias +0.03 *base_bias 
			pred =cl (sf (model .get ('bias',base_bias ),base_bias ),lo ,hi )*max (fc_now ,0.0 )
			model ['err']=0.92 *sf (model .get ('err',0.6 ),0.6 )+0.08 *abs (actual -pred )
			model ['samples']=si (model .get ('samples',0 ),0 )+1 

def analyze_exchange (exchange_rows ):

	def weighted_avg (num ,den ):
		return None if den <=1e-09 else num /den 
	stats ={'buy':{'asked':0.0 ,'contracted':0.0 ,'instant':0.0 ,'weighted_asked':0.0 ,'weighted_contracted':0.0 },'sell':{'asked':0.0 ,'contracted':0.0 ,'instant':0.0 ,'weighted_asked':0.0 ,'weighted_contracted':0.0 },'flat':{'asked':0.0 ,'contracted':0.0 ,'instant':0.0 ,'weighted_asked':0.0 ,'weighted_contracted':0.0 }}
	for row in exchange_rows :
		side =row .get ('side','flat')
		bucket =stats .get (side ,stats ['flat'])
		asked =abs (row ['askedAmount'])
		contracted =abs (row ['contractedAmount'])
		instant =abs (row ['instantAmount'])
		asked_price =row ['askedPrice']
		contracted_price =row ['contractedPrice']
		bucket ['asked']+=asked 
		bucket ['contracted']+=contracted 
		bucket ['instant']+=instant 
		bucket ['weighted_asked']+=asked *asked_price 
		bucket ['weighted_contracted']+=contracted *contracted_price 
	sell_avg_contracted =weighted_avg (stats ['sell']['weighted_contracted'],stats ['sell']['contracted'])
	sell_fill =None 
	if stats ['sell']['asked']>1e-09 :
		sell_fill =stats ['sell']['contracted']/stats ['sell']['asked']
	return {'buy_asked':stats ['buy']['asked'],'buy_contracted':stats ['buy']['contracted'],'buy_instant':stats ['buy']['instant'],'buy_avg_asked_price':weighted_avg (stats ['buy']['weighted_asked'],stats ['buy']['asked']),'buy_avg_contracted_price':weighted_avg (stats ['buy']['weighted_contracted'],stats ['buy']['contracted']),'sell_asked':stats ['sell']['asked'],'sell_contracted':stats ['sell']['contracted'],'sell_instant':stats ['sell']['instant'],'sell_avg_asked_price':weighted_avg (stats ['sell']['weighted_asked'],stats ['sell']['asked']),'sell_avg_contracted_price':sell_avg_contracted ,'sell_fill_ratio':sell_fill ,'instant_abs_total':stats ['buy']['instant']+stats ['sell']['instant']+stats ['flat']['instant']}

def _read_field (raw ,key ):
	if isinstance (raw ,dict ):
		return raw .get (key )
	return getattr (raw ,key ,None )

def _pick_first_float (raw ,keys ):
	for key in keys :
		value =sf (_read_field (raw ,key ),None )
		if value is not None :
			return value 
	return None 

def _normalize_side (raw ):
	if raw is None :
		return None 
	side =str (raw ).strip ().lower ()
	if 'buy'in side or 'bid'in side or 'purchase'in side :
		return 'buy'
	if 'sell'in side or 'ask'in side or 'offer'in side :
		return 'sell'
	return None 

def _extract_exchange_log_price (raw ):
	scalar =sf (raw ,None )
	if scalar is not None and 0.0 <scalar <=PMX *2.0 :
		return scalar 
	price_keys =('price','tariff','clearingPrice','marketPrice','value','avgPrice','dealPrice','askedPrice','contractedPrice','buyPrice','sellPrice','bidPrice','askPrice','bestBid','bestAsk','bid','ask')
	price =_pick_first_float (raw ,price_keys )
	if price is not None and 0.0 <price <=PMX *2.0 :
		return price 
	if isinstance (raw ,dict ):
		values =raw .values ()
	elif isinstance (raw ,(list ,tuple )):
		values =raw 
	else :
		values =(getattr (raw ,key ,None )for key in ('bestBid','bestAsk','price','tariff','clearingPrice','marketPrice','value'))
	for value in values :
		price =sf (value ,None )
		if price is not None and 0.0 <price <=PMX *2.0 :
			return price 
	return None 

def extract_exchange_log_prices (exchange_log ,limit =24 ):
	out =[]
	for raw in reversed (list (exchange_log or [])):
		price =_extract_exchange_log_price (raw )
		if price is None or price <=0.0 :
			continue 
		out .append (price )
		if len (out )>=max (1 ,si (limit ,24 )):
			break 
	out .reverse ()
	return out 

def get_exchange_reports (psm ):
	if isinstance (psm ,dict ):
		if 'exchangeReports'in psm :
			return list (psm .get ('exchangeReports',[]))
		cargo =psm .get ('data',{}).get ('contents',{}).get ('cargo',{})if isinstance (psm .get ('data',{}),dict )else {}
		if isinstance (cargo ,dict ):
			return list (cargo .get ('exchangeReports',[]))
		return []
	raw =getattr (psm ,'raw_data',None )
	if isinstance (raw ,dict ):
		return list (raw .get ('exchangeReports',[]))
	return []

def get_exchange_tickets (psm ):
	if isinstance (psm ,dict ):
		if 'exchangeTickets'in psm :
			return list (psm .get ('exchangeTickets',[]))
		cargo =psm .get ('data',{}).get ('contents',{}).get ('cargo',{})if isinstance (psm .get ('data',{}),dict )else {}
		if isinstance (cargo ,dict ):
			return list (cargo .get ('exchangeTickets',[]))
		return []
	raw =getattr (psm ,'raw_data',None )
	if isinstance (raw ,dict ):
		return list (raw .get ('exchangeTickets',[]))
	return []

def _extract_market_records_from_entry (entry ,depth =0 ):
	if entry is None or depth >2 :
		return []
	out =[]
	side_fields =('side','orderT','orderType','ticketType','direction','kind','type')
	amount_fields =('askedAmount','amount','volume','qty','quantity','power')
	price_fields =('askedPrice','price','tariff','marketPrice','clearingPrice','contractedPrice','dealPrice','value')
	side =None 
	for key in side_fields :
		side =_normalize_side (_read_field (entry ,key ))
		if side is not None :
			break 
	amount =_pick_first_float (entry ,amount_fields )
	price =_pick_first_float (entry ,price_fields )
	if amount is not None and price is not None and (price >0.0 )and (price <=PMX *2.0 ):
		if side is None :
			side ='buy'if amount >0.0 else 'sell'if amount <0.0 else None 
		if side in ('buy','sell'):
			volume =abs (amount )
			if volume >1e-06 :
				out .append ((side ,volume ,price ))
	paired_specs =(('buy',('buyAmount','bidAmount','buyVolume','demandVolume','buyPower'),('buyPrice','bidPrice','bestBid','bid')),('sell',('sellAmount','askAmount','sellVolume','supplyVolume','sellPower'),('sellPrice','askPrice','bestAsk','ask')))
	for paired_side ,amount_keys ,price_keys in paired_specs :
		paired_amount =_pick_first_float (entry ,amount_keys )
		paired_price =_pick_first_float (entry ,price_keys )
		if paired_amount is None or paired_price is None :
			continue 
		if paired_amount <=1e-06 or paired_price <=0.0 or paired_price >PMX *2.0 :
			continue 
		out .append ((paired_side ,abs (paired_amount ),paired_price ))
	if out :
		return out 
	if isinstance (entry ,dict ):
		nested =list (entry .values ())
	elif isinstance (entry ,(list ,tuple )):
		nested =list (entry )
	else :
		nested =[]
	for value in nested :
		out .extend (_extract_market_records_from_entry (value ,depth =depth +1 ))
	return out 

def summarize_competition_book (raw_entries ,floor ,cap ):
	records =[]
	for entry in list (raw_entries or []):
		records .extend (_extract_market_records_from_entry (entry ))
	buy_volume =0.0 
	sell_volume =0.0 
	buy_weighted =0.0 
	sell_weighted =0.0 
	best_bid =None 
	best_ask =None 
	for side ,volume ,price in records :
		p =cl (price ,floor ,cap )
		if side =='buy':
			buy_volume +=volume 
			buy_weighted +=volume *p 
			best_bid =p if best_bid is None else max (best_bid ,p )
		elif side =='sell':
			sell_volume +=volume 
			sell_weighted +=volume *p 
			best_ask =p if best_ask is None else min (best_ask ,p )
	total_volume =buy_volume +sell_volume 
	pressure =0.0 if total_volume <=1e-09 else cl ((buy_volume -sell_volume )/total_volume ,-1.0 ,1.0 )
	buy_ref =None if buy_volume <=1e-09 else buy_weighted /buy_volume 
	sell_ref =None if sell_volume <=1e-09 else sell_weighted /sell_volume 
	if buy_ref is None and best_bid is not None :
		buy_ref =best_bid 
	if sell_ref is None and best_ask is not None :
		sell_ref =best_ask 
	return {'buy_ref':buy_ref ,'sell_ref':sell_ref ,'best_bid':best_bid ,'best_ask':best_ask ,'pressure':pressure ,'activity':total_volume ,'records':len (records )}

def update_competition_history (state ,tick ,competition_book ):
	history =list (state .get ('competition_history',[]))
	history .append ({'tick':tick ,'buy_ref':sf (competition_book .get ('buy_ref',None ),None ),'sell_ref':sf (competition_book .get ('sell_ref',None ),None ),'best_bid':sf (competition_book .get ('best_bid',None ),None ),'best_ask':sf (competition_book .get ('best_ask',None ),None ),'pressure':sf (competition_book .get ('pressure',0.0 ),0.0 ),'activity':sf (competition_book .get ('activity',0.0 ),0.0 ),'records':si (competition_book .get ('records',0 ),0 )})
	competition_window =max (6 ,si (MI .get ('competition_window',14 ),14 ))
	history =history [-competition_window :]
	state ['competition_history']=history 
	return history 

def build_competition_context (state ):
	history =list (state .get ('competition_history',[]))
	active =[row for row in history if sf (row .get ('activity',0.0 ),0.0 )>1e-06 and si (row .get ('records',0 ),0 )>0 ]
	if not active :
		return {'has_data':False ,'pressure':0.0 ,'buy_ref':None ,'sell_ref':None ,'best_bid':None ,'best_ask':None ,'strong_buy_pressure':False ,'strong_sell_pressure':False }
	pressure_samples =[sf (row .get ('pressure',0.0 ),0.0 )for row in active ]
	buy_refs =[sf (row .get ('buy_ref',None ),None )for row in active if sf (row .get ('buy_ref',None ),None )is not None ]
	sell_refs =[sf (row .get ('sell_ref',None ),None )for row in active if sf (row .get ('sell_ref',None ),None )is not None ]
	bids =[sf (row .get ('best_bid',None ),None )for row in active if sf (row .get ('best_bid',None ),None )is not None ]
	asks =[sf (row .get ('best_ask',None ),None )for row in active if sf (row .get ('best_ask',None ),None )is not None ]
	pressure =avg (pressure_samples ,default =0.0 )
	buy_ref =avg (buy_refs ,default =None )if buy_refs else avg (bids ,default =None )if bids else None 
	sell_ref =avg (sell_refs ,default =None )if sell_refs else avg (asks ,default =None )if asks else None 
	best_bid =max (bids )if bids else buy_ref 
	best_ask =min (asks )if asks else sell_ref 
	buy_pressure_th =sf (MI .get ('competition_buy_pressure',0.2 ),0.2 )
	sell_pressure_th =sf (MI .get ('competition_sell_pressure',-0.22 ),-0.22 )
	recent_pressures =pressure_samples [-3 :]if len (pressure_samples )>=3 else pressure_samples 
	strong_buy_pressure =sum ((1 for p in recent_pressures if p >=buy_pressure_th ))>=max (1 ,len (recent_pressures )-1 )
	strong_sell_pressure =sum ((1 for p in recent_pressures if p <=sell_pressure_th ))>=max (1 ,len (recent_pressures )-1 )
	return {'has_data':True ,'pressure':cl (pressure ,-1.0 ,1.0 ),'buy_ref':buy_ref ,'sell_ref':sell_ref ,'best_bid':best_bid ,'best_ask':best_ask ,'strong_buy_pressure':strong_buy_pressure ,'strong_sell_pressure':strong_sell_pressure }

def summarize_market_prices (prices ,fallback ,floor ,cap ,alpha =0.34 ):
	clipped =[]
	for raw in prices :
		price =sf (raw ,None )
		if price is not None and price >0.0 :
			clipped .append (cl (price ,floor ,cap ))
	if not clipped :
		ref =cl (fallback ,floor ,cap )
		return {'count':0 ,'last':ref ,'ewma':ref ,'median':ref ,'rolling':ref ,'conservative':ref ,'aggressive':ref ,'reference':ref }
	last =clipped [-1 ]
	sorted_prices =sorted (clipped )
	mid =len (sorted_prices )//2 
	if len (sorted_prices )%2 ==1 :
		median =sorted_prices [mid ]
	else :
		median =0.5 *(sorted_prices [mid -1 ]+sorted_prices [mid ])
	a =cl (alpha ,0.05 ,0.95 )
	ewma =clipped [0 ]
	for price in clipped [1 :]:
		ewma =(1.0 -a )*ewma +a *price 
	rolling =0.5 *ewma +0.35 *median +0.15 *last 
	conservative =min (last ,median ,ewma )
	aggressive =max (last ,median ,ewma )
	reference =cl (rolling ,floor ,cap )
	return {'count':len (clipped ),'last':cl (last ,floor ,cap ),'ewma':cl (ewma ,floor ,cap ),'median':cl (median ,floor ,cap ),'rolling':cl (rolling ,floor ,cap ),'conservative':cl (conservative ,floor ,cap ),'aggressive':cl (aggressive ,floor ,cap ),'reference':reference }

def update_market_history (state ,tick ,market_stats ,execution_ratio =None ):
	history =list (state .get ('market_history',[]))
	history .append ({'tick':tick ,'sell_asked':sf (market_stats .get ('sell_asked',0.0 ),0.0 ),'sell_contracted':sf (market_stats .get ('sell_contracted',0.0 ),0.0 ),'fill_ratio':sf (market_stats .get ('sell_fill_ratio',0.0 ),0.0 ),'avg_ask_price':sf (market_stats .get ('sell_avg_asked_price',0.0 ),0.0 ),'avg_contracted_price':sf (market_stats .get ('sell_avg_contracted_price',0.0 ),0.0 ),'execution_ratio':sf (execution_ratio ,-1.0 )})
	history =history [-MARKET_HISTORY_WINDOW :]
	state ['market_history']=history 
	return history 

def build_market_context (state ):
	history =list (state .get ('market_history',[]))
	cfg =state .get ('cfg_runtime',{})if isinstance (state .get ('cfg_runtime',{}),dict )else {}
	step =max (0.01 ,sf (cfg .get ('exchangeConsumerPriceStep',0.2 ),0.2 ))
	floor =cl (sf (cfg .get ('exchangeExternalSell',PMN ),PMN ),PMN ,PMX )
	cap =cl (sf (cfg .get ('exchangeExternalBuy',PMX ),PMX ),floor +step ,PMX )
	fallback_ref =cl (sf (state .get ('ewma_market_price',state .get ('market_ref',max (floor +step ,cap -2.0 *step ))),max (floor +step ,cap -2.0 *step )),floor +step ,cap )
	market_log_window =max (4 ,si (MI .get ('market_log_window',12 ),12 ))
	market_log_prices =[sf (v ,None )for v in state .get ('exchange_price_history',[])]
	market_log_prices =[p for p in market_log_prices if p is not None and p >0.0 ][-market_log_window :]
	market_summary =summarize_market_prices (market_log_prices [-market_log_window :],fallback_ref ,floor +step ,cap ,alpha =sf (MI .get ('market_log_ewma_alpha',0.34 ),0.34 ))
	market_ref =sf (market_summary .get ('reference',fallback_ref ),fallback_ref )
	competition_ctx =build_competition_context (state )
	competition_pressure =cl (sf (competition_ctx .get ('pressure',0.0 ),0.0 ),-1.0 ,1.0 )
	competition_buy_ref =sf (competition_ctx .get ('buy_ref',None ),None )
	competition_sell_ref =sf (competition_ctx .get ('sell_ref',None ),None )
	competition_best_bid =sf (competition_ctx .get ('best_bid',competition_buy_ref ),competition_buy_ref )
	competition_best_ask =sf (competition_ctx .get ('best_ask',competition_sell_ref ),competition_sell_ref )
	strong_buy_pressure =bool (competition_ctx .get ('strong_buy_pressure',False ))
	strong_sell_pressure =bool (competition_ctx .get ('strong_sell_pressure',False ))
	if competition_buy_ref is not None and competition_buy_ref >0.0 :
		market_ref =cl (0.78 *market_ref +0.22 *competition_buy_ref ,floor +step ,cap )
	elif competition_best_bid is not None and competition_best_bid >0.0 :
		market_ref =cl (0.82 *market_ref +0.18 *competition_best_bid ,floor +step ,cap )
	fill_samples =[sf (row .get ('fill_ratio',0.0 ),0.0 )for row in history if sf (row .get ('sell_asked',0.0 ),0.0 )>MOV ]
	ask_samples =[sf (row .get ('avg_ask_price',0.0 ),0.0 )for row in history if sf (row .get ('sell_asked',0.0 ),0.0 )>MOV ]
	execution_samples =[sf (row .get ('execution_ratio',-1.0 ),-1.0 )for row in history if sf (row .get ('execution_ratio',-1.0 ),-1.0 )>=0.0 ]
	active_rows =[row for row in history if sf (row .get ('sell_asked',0.0 ),0.0 )>MOV ]
	last_row =active_rows [-1 ]if active_rows else None 
	has_fill_history =len (fill_samples )>=2 
	has_ask_history =len (ask_samples )>=1 
	has_execution_history =len (execution_samples )>=2 
	weak_fill_ratio =sf (MI .get ('weak_fill_ratio',0.78 ),0.78 )
	good_fill_ratio =sf (MI .get ('good_fill_ratio',0.92 ),0.92 )
	bad_fill_ratio =sf (MI .get ('bad_fill_ratio',0.58 ),0.58 )
	poor_execution_ratio =sf (MI .get ('poor_execution_ratio',0.35 ),0.35 )
	good_execution_ratio =sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
	near_zero_fill_ratio =sf (MI .get ('near_zero_fill_ratio',0.12 ),0.12 )
	near_zero_execution_ratio =sf (MI .get ('near_zero_execution_ratio',near_zero_fill_ratio ),near_zero_fill_ratio )
	over_gap =max (step ,sf (MI .get ('overpriced_gap_steps',1.1 ),1.1 )*step )
	under_gap =max (step ,sf (MI .get ('underpriced_gap_steps',1.6 ),1.6 )*step )
	neutral_fill =cl (max (sf (state .get ('fill_ratio_ewma',0.84 ),0.84 ),weak_fill_ratio +0.06 ),weak_fill_ratio +0.06 ,good_fill_ratio )
	neutral_execution =cl (max (sf (state .get ('execution_ratio_ewma',good_execution_ratio ),good_execution_ratio ),poor_execution_ratio +0.06 ),poor_execution_ratio +0.06 ,1.0 )
	execution_ewma =cl (sf (state .get ('execution_ratio_ewma',neutral_execution ),neutral_execution ),0.0 ,1.0 )
	recent_fill =avg (fill_samples ,default =neutral_fill )
	recent_ask =avg (ask_samples ,default =market_ref )
	recent_execution =avg (execution_samples ,default =neutral_execution )
	execution_signal =cl (0.64 *recent_execution +0.36 *execution_ewma if execution_samples else execution_ewma ,0.0 ,1.0 )
	last_ask =sf (last_row .get ('avg_ask_price',recent_ask ),recent_ask )if last_row else recent_ask 
	last_fill =sf (last_row .get ('fill_ratio',recent_fill ),recent_fill )if last_row else recent_fill 
	execution_rows =[row for row in history if sf (row .get ('execution_ratio',-1.0 ),-1.0 )>=0.0 ]
	last_execution_row =execution_rows [-1 ]if execution_rows else None 
	last_execution =sf (last_execution_row .get ('execution_ratio',execution_signal ),execution_signal )if last_execution_row else execution_signal 
	weak_execution =has_execution_history and execution_signal <poor_execution_ratio 
	ask_gap =recent_ask -market_ref 
	last_gap =last_ask -market_ref 
	near_zero_fill =bool (last_row )and (last_fill <=near_zero_fill_ratio or recent_fill <=near_zero_fill_ratio )
	near_zero_execution =bool (last_execution_row )and (last_execution <=near_zero_execution_ratio or execution_signal <=near_zero_execution_ratio )
	overpriced =bool (last_row )and (last_gap >=over_gap or (strong_sell_pressure and last_gap >=0.55 *over_gap ))and (last_fill <weak_fill_ratio or recent_fill <weak_fill_ratio or weak_execution or near_zero_fill or near_zero_execution or strong_sell_pressure )
	underpriced =bool (last_row )and (not overpriced )and (not near_zero_execution )and (last_gap <=-under_gap )and (last_fill >=good_fill_ratio )and (recent_fill >=good_fill_ratio -0.04 )and (not has_execution_history or execution_signal >=good_execution_ratio -0.04 )
	price_realism =1.0 
	if has_ask_history and ask_gap >0.0 :
		price_realism *=cl (1.0 -ask_gap /max (3.0 *step ,1.2 ),0.18 ,1.0 )
	if competition_ctx .get ('has_data',False ):
		price_realism *=cl (1.0 +0.34 *competition_pressure ,0.66 ,1.08 )
	if has_fill_history and recent_fill <weak_fill_ratio :
		price_realism *=cl (0.65 +0.55 *recent_fill /max (weak_fill_ratio ,1e-06 ),0.22 ,1.0 )
	if has_execution_history and execution_signal <poor_execution_ratio :
		price_realism *=cl (0.5 +0.8 *execution_signal /max (poor_execution_ratio ,1e-06 ),0.2 ,1.0 )
	if near_zero_fill :
		price_realism *=0.58 
	if near_zero_execution :
		price_realism *=0.52 
	if underpriced and recent_fill >=good_fill_ratio :
		price_realism *=1.06 
	price_realism =cl (price_realism ,0.12 ,1.0 )
	pricing_bias_steps =0.0 
	if overpriced :
		gap_steps =max (0.0 ,last_gap /max (step ,1e-06 ))
		pricing_bias_steps -=cl (0.8 +0.45 *gap_steps ,0.8 ,2.8 )
	if near_zero_fill :
		pricing_bias_steps -=1.0 
	if underpriced :
		gap_steps =max (0.0 ,-last_gap /max (step ,1e-06 ))
		pricing_bias_steps +=cl (0.25 +0.25 *gap_steps ,0.25 ,1.45 )
	if has_fill_history and (not overpriced )and (recent_fill >=good_fill_ratio )and (ask_gap <=0.4 *step ):
		pricing_bias_steps +=0.2 
	if has_fill_history and recent_fill <bad_fill_ratio :
		pricing_bias_steps -=0.5 
	if strong_buy_pressure and (not overpriced )and (not near_zero_execution ):
		pricing_bias_steps +=0.14 
	if strong_sell_pressure :
		pricing_bias_steps -=0.2 
	adaptive_bias =cl (sf (state .get ('sell_bias_steps',0.0 ),0.0 ),-2.8 ,1.2 )
	pricing_bias_steps =cl (pricing_bias_steps +adaptive_bias ,-3.4 ,2.2 )
	return {'market_ref':market_ref ,'market_rolling_ref':sf (market_summary .get ('rolling',market_ref ),market_ref ),'market_price_step':step ,'recent_fill_ratio':recent_fill ,'recent_ask_price':recent_ask ,'recent_execution_ratio':recent_execution ,'execution_signal_ratio':execution_signal ,'price_realism':price_realism ,'has_fill_history':has_fill_history ,'has_execution_history':has_execution_history ,'good_fill':has_fill_history and recent_fill >=good_fill_ratio ,'weak_fill':has_fill_history and recent_fill <weak_fill_ratio ,'weak_execution':weak_execution ,'near_zero_fill':near_zero_fill ,'near_zero_execution':near_zero_execution ,'overpriced':overpriced ,'underpriced':underpriced ,'pricing_bias_steps':pricing_bias_steps ,'competition_pressure':competition_pressure ,'competition_buy_ref':competition_buy_ref ,'competition_sell_ref':competition_sell_ref ,'competition_best_bid':competition_best_bid ,'competition_best_ask':competition_best_ask ,'competition_has_data':bool (competition_ctx .get ('has_data',False )),'competition_strong_buy':strong_buy_pressure ,'competition_strong_sell':strong_sell_pressure }

def compute_useful_energy (total_generated ,total_losses ):
	return total_generated -total_losses 

def compute_balance_energy (total_generated ,total_consumed ,total_losses ):
	return total_generated -total_consumed -total_losses 

def compute_offer_cap (state ,cfg ,tick ,useful_supply_now ):
	prev_useful =state .get ('prev_useful_energy_actual')
	if prev_useful is None :
		if tick ==0 and STRICT_FIRST_TICK_CAP :
			return sf (cfg ['exchangeAmountBuffer'],10.0 )
		return max (sf (cfg ['exchangeAmountBuffer'],10.0 ),useful_supply_now )
	return max (0.0 ,sf (prev_useful ,0.0 ))*sf (cfg ['exchangeAmountScaler'],1.2 )+sf (cfg ['exchangeAmountBuffer'],10.0 )

def predict_object_generation (state ,row ,fc_sun ,fc_wind ,sun_spread ,wind_spread ,cfg =None ,weather =None ,step_ahead =1 ):
	key =row ['address']
	typ =row ['type']
	cfg =cfg or {}
	weather =weather or {}
	delay =max (0 ,si (cfg .get ('weatherEffectsDelay',0 ),0 ))
	blend =1.0 if step_ahead >delay else step_ahead /max (1.0 ,float (delay +1 ))
	if typ =='solar':
		model =get_model (state ,key ,'solar')
		current_sun =max (0.0 ,sf (weather .get ('sun',0.0 ),0.0 ))
		eff_sun =max (0.0 ,(1.0 -blend )*current_sun +blend *max (0.0 ,fc_sun -0.2 *sun_spread ))
		pred =sf (model .get ('factor',0.65 ),0.65 )*eff_sun 
		pred *=cl (sf (model .get ('strength_bias',1.0 ),1.0 ),0.75 ,1.08 )
		pred *=cl (1.0 -0.03 *sf (model .get ('err',0.8 ),0.8 ),0.82 ,1.0 )
		return cl (pred ,0.0 ,sf (cfg .get ('maxSolarPower',20.0 ),20.0 ))
	if typ =='wind':
		model =get_model (state ,key ,'wind')
		current_wind =max (0.0 ,sf (weather .get ('wind',0.0 ),0.0 ))
		current_rot =max (0.0 ,sf (row .get ('wind_rotation',0.0 ),0.0 ))
		max_wind_power =sf (cfg .get ('maxWindPower',20.0 ),20.0 )
		safe_wind =max (0.0 ,fc_wind -0.35 *wind_spread )
		eff_wind =max (0.0 ,(1.0 -blend )*current_wind +blend *safe_wind )
		rot_ratio =sf (model .get ('wind_to_rot',0.04 ),0.04 )
		inertia =cl (1.0 -1.0 /max (2.0 ,float (delay +step_ahead +1 )),0.45 ,0.88 )
		projected_rot =max (0.0 ,inertia *current_rot +(1.0 -inertia )*rot_ratio *eff_wind )
		direct =sf (model .get ('factor',0.0048 ),0.0048 )*eff_wind **3 
		rot_based =sf (model .get ('rot_factor',80.0 ),80.0 )*projected_rot **3 
		curve_based =estimate_wind_from_curve (model ,projected_rot )
		pred =0.62 *direct +0.38 *rot_based 
		if curve_based is not None :
			pred =0.28 *direct +0.24 *rot_based +0.48 *curve_based 
		max_seen =sf (model .get ('max_power_seen',0.0 ),0.0 )
		if max_seen >0.0 :
			pred =min (pred ,min (max_wind_power ,1.1 *max_seen +0.4 ))
		pred *=cl (sf (model .get ('strength_bias',1.0 ),1.0 ),0.72 ,1.1 )
		pred *=cl (1.0 -0.05 *sf (model .get ('err',2.5 ),2.5 ),0.7 ,1.0 )
		failed_now =si (row .get ('failed',0 ),0 )
		if failed_now >0 :
			pred *=0.6 
		elif si (model .get ('last_failed',0 ),0 )>0 :
			pred *=0.82 
		wind_limit =sf (cfg .get ('weatherMaxWind',15.0 ),15.0 )
		storm_risk =sf (model .get ('storm_risk',0.0 ),0.0 )
		if eff_wind >wind_limit *0.85 :
			pred *=cl (0.92 -0.12 *storm_risk ,0.72 ,0.94 )
		return cl (pred ,0.0 ,max_wind_power )
	return 0.0 

def predict_object_load (state ,row ,forecast_value ,tick ):
	key =row ['address']
	typ =row ['type']
	if typ not in OTF :
		return 0.0 
	model =get_model (state ,key ,'load')
	type_prior =get_type_load_prior (state ,typ )
	lo ,hi =effective_load_bounds (state ,typ ,tick )
	base_bias =cl (blended_load_base_bias (state ,typ ,tick ,type_prior =type_prior ),lo ,hi )
	model_bias =sf (model .get ('bias',base_bias ),base_bias )
	model_bias =cl (model_bias ,lo ,hi )
	trust =effective_load_trust (state ,model ,typ ,tick )
	bias =trust *model_bias +(1.0 -trust )*base_bias 
	return max (0.0 ,forecast_value *cl (bias ,lo ,hi ))

def forecast_window (state ,object_rows ,bundle ,tick ,game_length ,horizon ):
	runtime_cfg =state .get ('cfg_runtime',{})
	wind_spread =max (get_forecast_spread (bundle ,'wind',0.0 ),sf (runtime_cfg .get ('corridorWind',0.5 ),0.5 ))
	sun_spread =max (get_forecast_spread (bundle ,'sun',0.0 ),sf (runtime_cfg .get ('corridorSun',0.5 ),0.5 ))
	weather =state .get ('weather_runtime',{})
	out =[]
	validated_rows =forecast_validated_rows (bundle ,game_length )
	if validated_rows <=0 :
		return out 
	last_tick =max (0 ,min (game_length ,validated_rows )-1 )
	start_tick =min (last_tick ,tick +1 )
	end_tick =min (last_tick ,tick +horizon )
	for t in range (start_tick ,end_tick +1 ):
		step_ahead =max (1 ,t -tick )
		fc_sun =get_forecast_value (bundle ,'sun',t )
		fc_wind =get_forecast_value (bundle ,'wind',t )
		total_gen =0.0 
		total_load =0.0 
		type_totals ={}
		for row in object_rows :
			gen_pred =predict_object_generation (state ,row ,fc_sun ,fc_wind ,sun_spread ,wind_spread ,cfg =runtime_cfg ,weather =weather ,step_ahead =step_ahead )
			total_gen +=gen_pred 
			type_totals .setdefault (row ['type'],{'gen':0.0 ,'load':0.0 })
			type_totals [row ['type']]['gen']+=gen_pred 
		for row in object_rows :
			typ =row ['type']
			fc_name =OTF .get (typ )
			if not fc_name :
				continue 
			load_pred =predict_object_load (state ,row ,get_forecast_value (bundle ,fc_name ,t ),t )
			total_load +=load_pred 
			type_totals .setdefault (typ ,{'gen':0.0 ,'load':0.0 })
			type_totals [typ ]['load']+=load_pred 
		loss_pred =predict_total_losses (state ,total_gen ,total_load )
		useful_supply_pred =max (0.0 ,total_gen -loss_pred )
		balance_pred =total_gen -total_load -loss_pred 
		out .append ({'tick':t ,'sun':fc_sun ,'wind':fc_wind ,'total_gen_pred':total_gen ,'total_load_pred':total_load ,'total_loss_pred':loss_pred ,'balance_pred':balance_pred ,'useful_supply_pred':useful_supply_pred ,'type_totals':type_totals })
	return out 

def percentile (values ,q ):
	if not values :
		return 0.0 
	vals =sorted ((float (v )for v in values ))
	if len (vals )==1 :
		return vals [0 ]
	q =cl (float (q ),0.0 ,1.0 )
	pos =q *(len (vals )-1 )
	lo =int (pos )
	hi =min (len (vals )-1 ,lo +1 )
	frac =pos -lo 
	return vals [lo ]*(1.0 -frac )+vals [hi ]*frac 

def contiguous_windows (flags ,min_len =1 ):
	out =[]
	start =None 
	for i ,flag in enumerate (flags ):
		if flag and start is None :
			start =i 
		elif not flag and start is not None :
			if i -start >=min_len :
				out .append ((start ,i -1 ))
			start =None 
	if start is not None and len (flags )-start >=min_len :
		out .append ((start ,len (flags )-1 ))
	return out 

def build_forecast_profile (state ,bundle ,object_rows ,game_length ):
	rows =min (game_length ,forecast_validated_rows (bundle ,game_length ))
	if rows <=0 :
		return {'rows':0 ,'ticks':[],'windows':{}}
	sun =[get_forecast_value (bundle ,'sun',t )for t in range (rows )]
	wind =[get_forecast_value (bundle ,'wind',t )for t in range (rows )]
	load =[]
	for t in range (rows ):
		total =0.0 
		for row in object_rows :
			fc_name =OTF .get (row .get ('type'))
			if fc_name :
				total +=predict_object_load (state ,row ,get_forecast_value (bundle ,fc_name ,t ),t )
		load .append (total )

	def _stats (vals ):
		if not vals :
			return (0.0 ,1.0 )
		mean =sum (vals )/max (1 ,len (vals ))
		var =sum (((v -mean )**2 for v in vals ))/max (1 ,len (vals ))
		return (mean ,max (var **0.5 ,1e-06 ))
	sun_mean ,sun_std =_stats (sun )
	wind_mean ,wind_std =_stats (wind )
	load_mean ,load_std =_stats (load )
	sun_q55 =percentile (sun ,0.55 )
	sun_q70 =percentile (sun ,0.7 )
	wind_q30 =percentile (wind ,0.3 )
	wind_q70 =percentile (wind ,0.7 )
	load_q70 =percentile (load ,0.7 )
	load_q85 =percentile (load ,0.85 )
	ticks =[]
	for t in range (rows ):
		z_sun =(sun [t ]-sun_mean )/sun_std 
		z_wind =(wind [t ]-wind_mean )/wind_std 
		z_load =(load [t ]-load_mean )/load_std 
		solar_active =sun [t ]>=max (1.0 ,sun_q55 )
		solar_peak =sun [t ]>=max (2.0 ,sun_q70 )
		load_peak =load [t ]>=load_q70 
		load_extreme =load [t ]>=load_q85 
		wind_peak =wind [t ]>=wind_q70 
		wind_low =wind [t ]<=wind_q30 
		mixed_peak =solar_peak and load_peak 
		combo_score =0.52 *z_sun +0.33 *z_load +0.15 *z_wind 
		risk_score =0.5 *max (0.0 ,-z_sun )+0.18 *max (0.0 ,-z_wind )+0.42 *max (0.0 ,z_load )
		ticks .append ({'tick':t ,'sun':sun [t ],'wind':wind [t ],'load':load [t ],'solar_active':solar_active ,'solar_peak':solar_peak ,'load_peak':load_peak ,'load_extreme':load_extreme ,'wind_peak':wind_peak ,'wind_low':wind_low ,'mixed_peak':mixed_peak ,'combo_score':combo_score ,'risk_score':risk_score ,'tail_low_load':t >=rows -8 and load [t ]<=load_mean ,'charge_bias':1.0 if mixed_peak or combo_score >=0.8 else 0.0 ,'protect_bias':1.0 if risk_score >=0.8 or (wind_low and load_peak and (not solar_active ))else 0.0 })
	windows ={'solar_active':contiguous_windows ([x ['solar_active']for x in ticks ],min_len =3 ),'mixed_peak':contiguous_windows ([x ['mixed_peak']for x in ticks ],min_len =2 ),'wind_peak':contiguous_windows ([x ['wind_peak']for x in ticks ],min_len =2 ),'risk_peak':contiguous_windows ([x ['protect_bias']>0.5 for x in ticks ],min_len =2 )}
	return {'rows':rows ,'ticks':ticks ,'windows':windows }

def forecast_profile_context (profile ,tick ,horizon =12 ):
	ticks =profile .get ('ticks',[])if isinstance (profile ,dict )else []
	if not ticks :
		return {'current':{},'avg_combo_6':0.0 ,'avg_combo_12':0.0 ,'avg_risk_6':0.0 ,'avg_risk_12':0.0 ,'next_mixed_in':None ,'next_risk_in':None ,'next_solar_in':None }
	idx =int (cl (tick ,0 ,len (ticks )-1 ))
	cur =ticks [idx ]

	def _avg (name ,n ):
		end =min (len (ticks ),idx +n +1 )
		vals =[sf (t .get (name ,0.0 ),0.0 )for t in ticks [idx :end ]]
		return sum (vals )/max (1 ,len (vals ))
	next_mixed_in =next ((i -idx for i in range (idx ,len (ticks ))if ticks [i ].get ('mixed_peak')),None )
	next_risk_in =next ((i -idx for i in range (idx ,len (ticks ))if ticks [i ].get ('protect_bias',0.0 )>0.5 ),None )
	next_solar_in =next ((i -idx for i in range (idx ,len (ticks ))if ticks [i ].get ('solar_active')),None )
	return {'current':cur ,'avg_combo_6':_avg ('combo_score',6 ),'avg_combo_12':_avg ('combo_score',max (12 ,horizon )),'avg_risk_6':_avg ('risk_score',6 ),'avg_risk_12':_avg ('risk_score',max (12 ,horizon )),'next_mixed_in':next_mixed_in ,'next_risk_in':next_risk_in ,'next_solar_in':next_solar_in }

def forecast_market_pressure (future ,cfg ,profile_ctx =None ):
	if not future :
		return {'forecast_renewable_level':0.5 ,'forecast_oversupply':0.5 ,'forecast_tightness':0.5 ,'forecast_balance_avg':0.0 ,'forecast_balance_min':0.0 ,'forecast_useful_avg':0.0 }
	window =future [:max (1 ,min (6 ,len (future )))]
	max_sun =max (1.0 ,sf (cfg .get ('weatherMaxSun',15.0 ),15.0 ))
	max_wind =max (1.0 ,sf (cfg .get ('weatherMaxWind',15.0 ),15.0 ))
	sun_level =avg ([cl (max (0.0 ,sf (row .get ('sun',0.0 ),0.0 ))/max_sun ,0.0 ,1.4 )for row in window ],default =0.0 )
	wind_level =avg ([cl (max (0.0 ,sf (row .get ('wind',0.0 ),0.0 ))/max_wind ,0.0 ,1.4 )for row in window ],default =0.0 )
	renewable_level =cl (0.56 *sun_level +0.44 *wind_level ,0.0 ,1.2 )
	balances =[sf (row .get ('balance_pred',0.0 ),0.0 )for row in window ]
	useful_supply =[sf (row .get ('useful_supply_pred',0.0 ),0.0 )for row in window ]
	balance_avg =avg (balances ,default =0.0 )
	balance_min =min (balances )if balances else 0.0 
	useful_avg =avg (useful_supply ,default =0.0 )
	oversupply =cl (0.62 *renewable_level +0.24 *cl (balance_avg /4.0 ,0.0 ,1.0 )+0.14 *cl (useful_avg /8.0 ,0.0 ,1.0 ),0.0 ,1.0 )
	tightness =cl (0.58 *(1.0 -cl (renewable_level ,0.0 ,1.0 ))+0.27 *cl (-balance_avg /6.0 ,0.0 ,1.0 )+0.15 *cl (-balance_min /7.0 ,0.0 ,1.0 ),0.0 ,1.0 )
	if profile_ctx :
		avg_combo =sf (profile_ctx .get ('avg_combo_12',0.0 ),0.0 )
		avg_risk =sf (profile_ctx .get ('avg_risk_12',0.0 ),0.0 )
		if avg_risk >avg_combo +0.12 :
			tightness =cl (tightness +0.08 ,0.0 ,1.0 )
		elif avg_combo >avg_risk +0.12 :
			oversupply =cl (oversupply +0.05 ,0.0 ,1.0 )
	return {'forecast_renewable_level':cl (renewable_level ,0.0 ,1.0 ),'forecast_oversupply':oversupply ,'forecast_tightness':tightness ,'forecast_balance_avg':balance_avg ,'forecast_balance_min':balance_min ,'forecast_useful_avg':useful_avg }

def compute_target_soc (total_capacity ,future ,fill_ratio ,tick ,game_length ,profile_ctx =None ,market_ctx =None ):
	base_ceil =min (total_capacity ,total_capacity *SOC_CEIL_FRAC )
	weighted_gap =0.0 
	weighted_surplus =0.0 
	raw_gap_sum =0.0 
	raw_surplus_sum =0.0 
	useful_gap_sum =0.0 
	max_gap =0.0 
	solar_preds =[]
	wind_preds =[]
	for i ,row in enumerate (future ):
		w =1.0 /(i +1 )
		bal =sf (row .get ('balance_pred',0.0 ),0.0 )
		useful =sf (row .get ('useful_supply_pred',0.0 ),0.0 )
		gap =max (0.0 ,-bal )
		surplus =max (0.0 ,bal )
		weighted_gap +=w *gap 
		weighted_surplus +=w *surplus 
		raw_gap_sum +=gap 
		raw_surplus_sum +=surplus 
		max_gap =max (max_gap ,gap )
		useful_gap_sum +=w *max (0.0 ,1.5 -useful )
		type_totals =row .get ('type_totals',{})if isinstance (row .get ('type_totals',{}),dict )else {}
		solar_preds .append (sf (type_totals .get ('solar',{}).get ('gen',0.0 ),0.0 ))
		wind_preds .append (sf (type_totals .get ('wind',{}).get ('gen',0.0 ),0.0 ))
	chronic_deficit =raw_gap_sum >max (4.0 ,1.5 *raw_surplus_sum )
	floor =total_capacity *(0.06 if chronic_deficit else SOC_FLOOR_FRAC )
	target =floor +0.72 *weighted_gap +0.18 *useful_gap_sum -0.08 *weighted_surplus 
	target +=0.02 *total_capacity 
	if fill_ratio <0.6 and raw_surplus_sum >0.0 :
		target +=0.05 *total_capacity 
	elif fill_ratio >0.9 and raw_surplus_sum >raw_gap_sum :
		target -=0.03 *total_capacity 
	if profile_ctx :
		current =profile_ctx .get ('current',{})
		avg_combo =sf (profile_ctx .get ('avg_combo_12',0.0 ),0.0 )
		avg_risk =sf (profile_ctx .get ('avg_risk_12',0.0 ),0.0 )
		next_risk_in =profile_ctx .get ('next_risk_in')
		next_mixed_in =profile_ctx .get ('next_mixed_in')
		if current .get ('mixed_peak')or current .get ('solar_active'):
			target =max (target ,0.36 *total_capacity )
		if next_risk_in is not None and 0 <=next_risk_in <=20 :
			risk_floor =(0.62 -0.014 *min (next_risk_in ,20 ))*total_capacity 
			target =max (target ,risk_floor )
		if avg_risk >avg_combo +0.12 :
			target =max (target ,0.42 *total_capacity )
		if next_mixed_in is not None and 0 <=next_mixed_in <=10 and (next_risk_in is not None )and (next_risk_in <=20 ):
			target =max (target ,0.5 *total_capacity )
		if current .get ('tail_low_load'):
			target -=0.04 *total_capacity 
	solar_now =solar_preds [0 ]if solar_preds else 0.0 
	solar_later =avg (solar_preds [1 :4 ],default =solar_now )
	solar_drop_risk =max (0.0 ,solar_now -solar_later )
	wind_peak =max (wind_preds [:4 ],default =0.0 )
	if solar_drop_risk >0.8 :
		target +=min (0.18 *total_capacity ,0.16 *solar_drop_risk )
	if wind_peak >12.0 :
		target +=min (0.14 *total_capacity ,0.02 *wind_peak *total_capacity /max (total_capacity ,1.0 ))
	if market_ctx :
		price_realism =sf (market_ctx .get ('price_realism',1.0 ),1.0 )
		recent_fill =sf (market_ctx .get ('recent_fill_ratio',fill_ratio ),fill_ratio )
		weak_fill_ratio =sf (MI .get ('weak_fill_ratio',0.78 ),0.78 )
		has_fill_history =bool (market_ctx .get ('has_fill_history',False ))
		weak_market_fill =has_fill_history and recent_fill <weak_fill_ratio 
		anti_dump_headroom =sf (market_ctx .get ('anti_dump_headroom',0.0 ),0.0 )
		if weak_market_fill :
			target +=0.05 *total_capacity 
		if price_realism <0.6 :
			target +=0.04 *total_capacity 
		if anti_dump_headroom <2.0 and raw_surplus_sum >0.0 :
			target +=0.03 *total_capacity 
	target +=0.12 *max_gap 
	ticks_left =max (0 ,game_length -tick )
	if ticks_left <=ENDGAME_TICKS :
		floor =0.0 
		target =max (0.0 ,0.1 *weighted_gap )
	return cl (target ,floor ,base_ceil )

def storage_policy (state ,cfg ,storages ,balance_now ,future ,fill_ratio ,tick ,game_length ,loss_ratio =0.1 ,profile_ctx =None ,market_ctx =None ):
	if not storages :
		return ([],[],{'target_soc':0.0 ,'prep_soc':0.0 ,'total_soc':0.0 ,'soc_ceil':0.0 ,'charge_total':0.0 ,'discharge_total':0.0 ,'discharge_for_market':0.0 ,'chronic_deficit':False ,'floor_soc':0.0 ,'soc_band':0.0 ,'emergency_floor_soc':0.0 ,'working_floor_soc':0.0 ,'high_risk_target_soc':0.0 ,'allow_market_discharge':False ,'mode':'hold','signal':0.0 ,'protected_soc':0.0 ,'premium_sell_ready':False ,'fill_to_ceiling':False ,'deficit_relief_floor_soc':0.0 ,'deficit_relief_cap':0.0 })
	cell_capacity =sf (cfg ['cellCapacity'],120.0 )
	charge_rate =sf (cfg ['cellChargeRate'],15.0 )
	discharge_rate =sf (cfg ['cellDischargeRate'],20.0 )
	norm_storages =[{'id':s ['id'],'soc':clamp_storage_soc (s .get ('soc',0.0 ),cell_capacity )}for s in storages ]
	total_capacity =len (norm_storages )*cell_capacity 
	total_soc =sum ((s ['soc']for s in norm_storages ))
	total_charge_rate =len (norm_storages )*charge_rate 
	total_discharge_rate =len (norm_storages )*discharge_rate 
	soc_ceil =min (total_capacity ,total_capacity *SOC_CEIL_FRAC )
	target_soc =compute_target_soc (total_capacity ,future ,fill_ratio ,tick ,game_length ,profile_ctx =profile_ctx ,market_ctx =market_ctx )
	next_balance =sf (future [0 ].get ('balance_pred',0.0 ),0.0 )if future else 0.0 
	next2_balance =sf (future [1 ].get ('balance_pred',next_balance ),next_balance )if len (future )>1 else next_balance 
	signal =0.55 *balance_now +0.3 *next_balance +0.15 *next2_balance 
	deficit_sum =sum ((max (0.0 ,-sf (r .get ('balance_pred',0.0 ),0.0 ))for r in future ))
	surplus_sum =sum ((max (0.0 ,sf (r .get ('balance_pred',0.0 ),0.0 ))for r in future ))
	chronic_deficit =deficit_sum >max (4.0 ,1.5 *surplus_sum )
	floor_frac =0.06 if chronic_deficit else SOC_FLOOR_FRAC 
	if tick >=game_length -ENDGAME_TICKS :
		floor_frac =0.0 
	current_profile =profile_ctx .get ('current',{})if profile_ctx else {}
	next_risk_in =profile_ctx .get ('next_risk_in')if profile_ctx else None 
	next_mixed_in =profile_ctx .get ('next_mixed_in')if profile_ctx else None 
	severe_risk =bool (chronic_deficit and (loss_ratio >0.38 or current_profile .get ('protect_bias',0.0 )>0.5 or (next_risk_in is not None and 0 <=next_risk_in <=12 )))
	if next_risk_in is not None and 0 <=next_risk_in <=18 :
		floor_frac =max (floor_frac ,0.1 )
	floor_soc =total_capacity *floor_frac 
	recent_fill =sf (market_ctx .get ('recent_fill_ratio',fill_ratio ),fill_ratio )if market_ctx else fill_ratio 
	price_realism =sf (market_ctx .get ('price_realism',1.0 ),1.0 )if market_ctx else 1.0 
	overpriced_market =bool (market_ctx .get ('overpriced',False ))if market_ctx else False 
	has_fill_history =bool (market_ctx .get ('has_fill_history',False ))if market_ctx else False 
	weak_fill_ratio =sf (MI .get ('weak_fill_ratio',0.78 ),0.78 )
	good_fill_ratio =sf (MI .get ('good_fill_ratio',0.92 ),0.92 )
	preferred_ask_high =sf (MI .get ('preferred_ask_high',9.2 ),9.2 )
	weak_market_fill =has_fill_history and recent_fill <weak_fill_ratio 
	market_sell_support =recent_fill >=good_fill_ratio if has_fill_history else price_realism >=0.82 
	anti_dump_headroom =sf (market_ctx .get ('anti_dump_headroom',0.0 ),0.0 )if market_ctx else 0.0 
	recent_ask_price =sf (market_ctx .get ('recent_ask_price',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 ))if market_ctx else sf (state .get ('market_ref',4.8 ),4.8 )
	emergency_floor_soc =max (floor_soc ,total_capacity *(0.12 if severe_risk else 0.08 if chronic_deficit else 0.05 ))
	working_floor_soc =max (emergency_floor_soc ,total_capacity *(0.18 if weak_market_fill or price_realism <0.65 else 0.12 ))
	prep_soc =target_soc 
	if next_risk_in is not None and 0 <=next_risk_in <=18 :
		prep_soc =max (prep_soc ,(0.68 -0.016 *min (next_risk_in ,18 ))*total_capacity )
	elif current_profile .get ('mixed_peak')or current_profile .get ('solar_active'):
		prep_soc =max (prep_soc ,0.42 *total_capacity )
	if next_mixed_in is not None and 0 <=next_mixed_in <=8 and (next_risk_in is not None )and (0 <=next_risk_in <=18 ):
		prep_soc =max (prep_soc ,0.58 *total_capacity )
	if weak_market_fill or overpriced_market or anti_dump_headroom <2.0 :
		prep_soc =max (prep_soc ,working_floor_soc +0.08 *total_capacity )
	prep_soc =cl (prep_soc ,floor_soc ,soc_ceil )
	high_risk_target_soc =max (prep_soc ,total_capacity *(0.62 if severe_risk else 0.0 ))
	protected_soc =max (working_floor_soc ,0.88 *prep_soc if severe_risk else 0.8 *prep_soc if chronic_deficit else 0.72 *prep_soc )
	charge_total =0.0 
	discharge_total =0.0 
	discharge_for_market =0.0 
	current_deficit =max (0.0 ,-balance_now )
	current_surplus =max (0.0 ,balance_now )
	storage_gap =max (0.0 ,prep_soc -total_soc )
	charge_room =max (0.0 ,total_capacity *SOC_CEIL_FRAC -total_soc )
	charge_cap =max (0.0 ,prep_soc +0.02 *total_capacity -total_soc )
	discharge_floor_soc =cl (max (protected_soc +0.02 *total_capacity ,prep_soc -0.02 *total_capacity ),floor_soc ,soc_ceil )
	discharge_cap =max (0.0 ,total_soc -discharge_floor_soc )
	market_soc_floor =max (protected_soc +0.02 *total_capacity ,prep_soc +(0.04 if tick >=game_length -2 else 0.06 )*total_capacity )
	force_discharge =balance_now <-4.0 and total_soc >discharge_floor_soc +0.08 *total_capacity 
	force_charge =total_soc <floor_soc +0.06 *total_capacity and balance_now >=0.0 
	startup_mode_now =startup_active (state ,tick )
	premium_sell_ready =bool (market_sell_support and price_realism >=0.92 and (recent_ask_price >=preferred_ask_high )and (not has_fill_history or not weak_market_fill )and (not overpriced_market )and (anti_dump_headroom >=MOV ))
	full_charge_gap_trigger =max (MOV ,0.01 *total_capacity )
	fill_to_ceiling =current_surplus >=MOV and charge_room >full_charge_gap_trigger and (not premium_sell_ready )and (not force_discharge )
	deficit_relief_floor_soc =max (floor_soc ,emergency_floor_soc )
	deficit_relief_cap =max (0.0 ,total_soc -deficit_relief_floor_soc )
	urgent_deficit_discharge =current_deficit >=MOV and deficit_relief_cap >=MOV 
	recharge_gap_trigger =0.03 *total_capacity 
	topoff_gap_trigger =max (MOV ,0.006 *total_capacity )
	opportunistic_recharge =storage_gap >recharge_gap_trigger and current_surplus >=MOV and (charge_room >0.0 )and (charge_cap >0.0 )
	topoff_recharge =storage_gap >topoff_gap_trigger and current_surplus >=MOV and (charge_room >0.0 )and (charge_cap >0.0 )and (not force_discharge )and (startup_mode_now or signal >=-1.5 )
	desired_mode ='hold'
	if force_discharge :
		desired_mode ='discharge'
	elif urgent_deficit_discharge :
		desired_mode ='discharge'
	elif force_charge :
		desired_mode ='charge'
	elif opportunistic_recharge :
		desired_mode ='charge'
	elif topoff_recharge :
		desired_mode ='charge'
	elif fill_to_ceiling :
		desired_mode ='charge'
	elif total_soc >discharge_floor_soc +0.05 *total_capacity and signal <-2.0 :
		desired_mode ='discharge'
	mode =desired_mode 
	state ['storage_mode']=mode 
	if mode =='charge'and charge_room >0.0 and (current_surplus >0.0 ):
		charge_limit =charge_room if fill_to_ceiling else charge_cap 
		if topoff_recharge and (not opportunistic_recharge )and (not force_charge )and (not fill_to_ceiling ):
			charge_limit =min (charge_limit ,storage_gap )
		if charge_limit >0.0 :
			charge_total =min (total_charge_rate ,charge_room ,charge_limit ,current_surplus )
	effective_discharge_cap =discharge_cap 
	if force_discharge or urgent_deficit_discharge :
		effective_discharge_cap =max (effective_discharge_cap ,deficit_relief_cap )
	order_discharge_floor_soc =discharge_floor_soc 
	if force_discharge or urgent_deficit_discharge :
		order_discharge_floor_soc =min (order_discharge_floor_soc ,deficit_relief_floor_soc )
	if mode =='discharge'and effective_discharge_cap >0.0 :
		desired =current_deficit +0.25 *max (0.0 ,-next_balance )
		if chronic_deficit or current_profile .get ('protect_bias',0.0 )>0.5 :
			desired +=0.1 *deficit_sum 
		if severe_risk :
			desired *=0.78 
		if force_discharge :
			desired =max (desired ,current_deficit +0.5 *max (0.0 ,-next_balance ))
		if urgent_deficit_discharge :
			desired =max (desired ,current_deficit )
		discharge_total =min (total_discharge_rate ,effective_discharge_cap ,max (0.0 ,desired ))
	allow_market_discharge =tick >=game_length -ENDGAME_TICKS and total_soc >market_soc_floor and (current_deficit <MOV )and (signal >=-1.0 )and (not severe_risk )and (loss_ratio <0.3 )and (current_profile .get ('protect_bias',0.0 )<=0.5 )and (next_risk_in is None or next_risk_in >3 )and market_sell_support and (price_realism >=0.72 )and (not overpriced_market )and (anti_dump_headroom >=MOV )
	premium_market_window =total_soc >=soc_ceil -max (0.04 *total_capacity ,total_discharge_rate )and total_soc >market_soc_floor and (current_deficit <MOV )and (signal >=-0.4 )and (not severe_risk )and (loss_ratio <0.26 )and (current_profile .get ('protect_bias',0.0 )<=0.35 )and (next_risk_in is None or next_risk_in >5 )and market_sell_support and (price_realism >=0.9 )and (recent_ask_price >=preferred_ask_high )and (not weak_market_fill )and (not overpriced_market )and (anti_dump_headroom >=MOV )
	allow_market_discharge =allow_market_discharge or premium_market_window 
	if allow_market_discharge and mode !='charge':
		extra =min (max (0.0 ,total_discharge_rate -discharge_total ),max (0.0 ,discharge_cap -discharge_total ))
		market_headroom =max (0.0 ,total_soc -market_soc_floor )
		if market_headroom >0.0 :
			discharge_for_market =min (extra ,market_headroom )
			discharge_total +=discharge_for_market 
	charge_orders =[]
	discharge_orders =[]
	rem =charge_total 
	for s in sorted (norm_storages ,key =lambda x :x ['soc']):
		if rem <=1e-09 :
			break 
		room =max (0.0 ,cell_capacity -s ['soc'])
		amt =min (rem ,charge_rate ,room )
		if amt >=1e-09 :
			charge_orders .append ((s ['id'],round_vol (amt )))
			rem -=amt 
	rem =discharge_total 
	floor_per_cell =order_discharge_floor_soc /max (1 ,len (norm_storages ))
	for s in sorted (norm_storages ,key =lambda x :-x ['soc']):
		if rem <=1e-09 :
			break 
		avail =max (0.0 ,s ['soc']-floor_per_cell )
		amt =min (rem ,discharge_rate ,avail )
		if amt >=1e-09 :
			discharge_orders .append ((s ['id'],round_vol (amt )))
			rem -=amt 
	charge_total =sum ((v for _ ,v in charge_orders ))
	discharge_total =sum ((v for _ ,v in discharge_orders ))
	discharge_for_market =min (discharge_total ,max (0.0 ,discharge_for_market ))
	return (charge_orders ,discharge_orders ,{'target_soc':target_soc ,'prep_soc':prep_soc ,'total_soc':total_soc ,'soc_ceil':soc_ceil ,'charge_total':charge_total ,'discharge_total':discharge_total ,'discharge_for_market':discharge_for_market ,'chronic_deficit':chronic_deficit ,'floor_soc':floor_soc ,'soc_band':max (0.0 ,prep_soc -floor_soc ),'emergency_floor_soc':emergency_floor_soc ,'working_floor_soc':working_floor_soc ,'high_risk_target_soc':high_risk_target_soc ,'allow_market_discharge':allow_market_discharge ,'mode':mode ,'signal':signal ,'protected_soc':protected_soc ,'discharge_floor_soc':discharge_floor_soc ,'order_discharge_floor_soc':order_discharge_floor_soc ,'urgent_deficit_discharge':urgent_deficit_discharge ,'premium_sell_ready':premium_sell_ready ,'fill_to_ceiling':fill_to_ceiling ,'deficit_relief_floor_soc':deficit_relief_floor_soc ,'deficit_relief_cap':deficit_relief_cap ,'premium_market_window':premium_market_window ,'recent_ask_price':recent_ask_price })

def analyze_topology (object_rows ,network_rows ,total_generated ):

	def _decode (value ):
		if isinstance (value ,str ):
			try :
				return json .loads (value )if value else []
			except Exception :
				return None 
		return value 

	def _coerce_int (value ):
		try :
			if value is None :
				return None 
			return int (value )
		except Exception :
			return None 

	def _normalize_node_id (value ):
		value =_decode (value )
		if isinstance (value ,dict ):
			load =value .get ('load')
			idx =value .get ('int')
		elif isinstance (value ,(list ,tuple ))and len (value )>=2 and isinstance (value [0 ],str ):
			load =value [0 ]
			idx =value [1 ]
		else :
			load =getattr (value ,'load',None )
			idx =getattr (value ,'int',None )
		idx =_coerce_int (idx )
		if load is None or idx is None :
			return None 
		return f'{str (load ).strip ().lower ()}:{idx }'

	def _looks_like_segment (value ):
		value =_decode (value )
		if isinstance (value ,dict ):
			return 'line'in value and ('id'in value or ('load'in value and 'int'in value ))
		if isinstance (value ,(list ,tuple ))and len (value )>=2 :
			return _normalize_node_id (value [0 ])is not None and _coerce_int (value [1 ])is not None 
		return getattr (value ,'id',None )is not None and _coerce_int (getattr (value ,'line',None ))is not None 

	def _normalize_segment (value ):
		value =_decode (value )
		if isinstance (value ,dict ):
			node_key =_normalize_node_id (value .get ('id'))
			line =_coerce_int (value .get ('line'))
		elif isinstance (value ,(list ,tuple ))and len (value )>=2 :
			node_key =_normalize_node_id (value [0 ])
			line =_coerce_int (value [1 ])
		else :
			node_key =_normalize_node_id (getattr (value ,'id',None ))
			line =_coerce_int (getattr (value ,'line',None ))
		if not node_key or line is None or line <=0 :
			return None 
		return (node_key ,line )

	def _normalize_route (value ):
		value =_decode (value )
		if value is None :
			return None 
		if not isinstance (value ,(list ,tuple )):
			return None 
		route =[]
		for segment in value :
			normalized =_normalize_segment (segment )
			if normalized is None :
				return None 
			route .append (normalized )
		return route 

	def _normalize_object_routes (value ):
		value =_decode (value )
		if value is None :
			return ([],0 ,1 )
		if not isinstance (value ,(list ,tuple )):
			return ([],0 ,1 )
		if not value :
			return ([],1 ,0 )
		if value and _looks_like_segment (value [0 ]):
			candidates =[value ]
		else :
			candidates =list (value )
		routes =[]
		empty_count =0 
		broken_count =0 
		for candidate in candidates :
			candidate =_decode (candidate )
			if candidate ==[]or candidate ==():
				empty_count +=1 
				continue 
			route =_normalize_route (candidate )
			if route is None :
				broken_count +=1 
				continue 
			if not route :
				empty_count +=1 
				continue 
			routes .append (route )
		return (routes ,empty_count ,broken_count )

	def _route_key (route ):
		if not route :
			return 'root'
		return '>'.join ((f'{node_key }:{line }'for node_key ,line in route ))

	def _prefix_keys (route ):
		return [_route_key (route [:idx ])for idx in range (1 ,len (route ))]

	def _route_is_rooted (route ):
		return bool (route )and route [0 ][0 ].startswith ('main:')

	def _route_has_cycle (route ):
		nodes =[node_key for node_key ,_ in route ]
		return len (nodes )!=len (set (nodes ))

	def _first_route_depth (routes ):
		return len (routes [0 ])if routes else 0 
	warnings =[]
	vulnerabilities =[]

	def _add_warning (warning ,vulnerability =None ):
		if warning not in warnings :
			warnings .append (warning )
		target =vulnerability if vulnerability is not None else warning 
		if target and target not in vulnerabilities :
			vulnerabilities .append (target )
	by_branch ={}
	branch_mix ={}
	object_path_depths =[]
	hospital_inputs =0 
	factory_inputs =0 
	rootless_network_routes =set ()
	cyclic_network_routes =set ()
	duplicate_network_routes =set ()
	conflicting_network_routes =set ()
	missing_prefix_routes =set ()
	network_route_keys =set ()
	broken_network_rows =0 
	segment_parent_map ={}
	for row in network_rows :
		route =_normalize_route (row .get ('location',[]))
		if route is None :
			broken_network_rows +=1 
			_add_warning ('broken_network_routes','broken_network_routes')
			continue 
		if not route :
			continue 
		route_key =_route_key (route )
		if route_key in network_route_keys :
			duplicate_network_routes .add (route_key )
		network_route_keys .add (route_key )
		if not _route_is_rooted (route ):
			rootless_network_routes .add (route_key )
		if _route_has_cycle (route ):
			cyclic_network_routes .add (route_key )
		for idx ,(node_key ,line )in enumerate (route ):
			segment_key =f'{node_key }:{line }'
			parent_key =_route_key (route [:idx ])if idx >0 else '__root__'
			segment_parent_map .setdefault (segment_key ,set ()).add (parent_key )
		throughput =abs (sf (row .get ('upflow',0.0 ),0.0 ))+abs (sf (row .get ('downflow',0.0 ),0.0 ))
		bucket =by_branch .setdefault (route_key ,{'losses':0.0 ,'upflow':0.0 ,'downflow':0.0 ,'throughput':0.0 ,'count':0 })
		bucket ['losses']+=sf (row .get ('losses',0.0 ),0.0 )
		bucket ['upflow']+=sf (row .get ('upflow',0.0 ),0.0 )
		bucket ['downflow']+=sf (row .get ('downflow',0.0 ),0.0 )
		bucket ['throughput']+=throughput 
		bucket ['count']+=1 
	for route_key in list (network_route_keys ):
		for idx in range (1 ,len (route_key .split ('>'))):
			prefix_key ='>'.join (route_key .split ('>')[:idx ])
			if prefix_key not in network_route_keys :
				missing_prefix_routes .add (route_key )
				break 
	for segment_key ,parents in segment_parent_map .items ():
		non_root_parents ={parent for parent in parents if parent !='__root__'}
		if len (non_root_parents )>1 :
			conflicting_network_routes .add (segment_key )
	if duplicate_network_routes :
		_add_warning ('duplicate_network_routes','duplicate_network_routes')
	if conflicting_network_routes :
		_add_warning ('conflicting_network_routes','conflicting_network_routes')
	if rootless_network_routes :
		_add_warning ('routes_not_connected_to_main','routes_not_connected_to_main')
	if cyclic_network_routes :
		_add_warning ('cyclic_routes','cyclic_routes')
	if missing_prefix_routes :
		_add_warning ('disconnected_routes','disconnected_routes')

	def _is_valid_route (route ):
		route_key =_route_key (route )
		if route_key not in network_route_keys :
			return False 
		if not _route_is_rooted (route ):
			return False 
		if _route_has_cycle (route ):
			return False 
		return all ((prefix_key in network_route_keys for prefix_key in _prefix_keys (route )))
	islanded_objects =set ()
	broken_objects =set ()
	for row in object_rows :
		routes ,empty_count ,broken_count =_normalize_object_routes (row .get ('path',[]))
		object_path_depths .append (avg ([len (route )for route in routes ],default =0.0 ))
		address =row .get ('address','unknown')
		typ =row .get ('type')
		valid_route_keys =sorted ({_route_key (route )for route in routes if _is_valid_route (route )})
		invalid_route_count =0 
		missing_network_route_count =0 
		for route in routes :
			route_key =_route_key (route )
			if route_key not in network_route_keys :
				missing_network_route_count +=1 
				continue 
			if not _is_valid_route (route ):
				invalid_route_count +=1 
		if typ !='main'and (empty_count >0 or broken_count >0 or invalid_route_count >0 or (missing_network_route_count >0 )):
			broken_objects .add (address )
			_add_warning ('broken_object_paths','broken_object_paths')
			_add_warning (f'broken_path:{address }',f'broken_path:{address }')
		if typ !='main'and empty_count >0 :
			_add_warning ('empty_object_paths','empty_object_paths')
			_add_warning (f'empty_path:{address }',f'empty_path:{address }')
		if typ !='main'and missing_network_route_count >0 :
			_add_warning ('object_paths_not_in_network','object_paths_not_in_network')
			_add_warning (f'path_not_in_network:{address }',f'path_not_in_network:{address }')
		if typ !='main'and (not valid_route_keys ):
			islanded_objects .add (address )
		if typ =='hospital':
			hospital_inputs =max (hospital_inputs ,len (valid_route_keys ))
			if len (valid_route_keys )!=2 :
				_add_warning ('hospital_not_dual_fed','hospital_not_dual_fed')
		if typ =='factory':
			factory_inputs =max (factory_inputs ,len (valid_route_keys ))
			if len (valid_route_keys )==0 :
				_add_warning ('factory_missing_input','factory_missing_input')
			elif len (valid_route_keys )>1 :
				_add_warning ('factory_overconnected','factory_overconnected')
		for route_key in valid_route_keys :
			mix =branch_mix .setdefault (route_key ,{'gen':0 ,'load':0 ,'storage':0 ,'hospital':0 ,'factory':0 })
			if typ in ('solar','wind'):
				mix ['gen']+=1 
			elif typ in ('houseA','houseB','office','factory','hospital'):
				mix ['load']+=1 
			elif typ =='storage':
				mix ['storage']+=1 
			if typ =='hospital':
				mix ['hospital']+=1 
			if typ =='factory':
				mix ['factory']+=1 
	if islanded_objects :
		_add_warning ('islanded_objects','islanded_objects')
	branch_losses_sorted =sorted (({'branch':key ,**values }for key ,values in by_branch .items ()),key =lambda item :item ['losses'],reverse =True )
	total_branch_losses =sum ((item ['losses']for item in branch_losses_sorted ))
	total_throughput =sum ((item ['throughput']for item in branch_losses_sorted ))
	branch_concentration =0.0 
	if total_throughput >1e-09 :
		branch_concentration =sum (((item ['throughput']/total_throughput )**2 for item in branch_losses_sorted if item ['throughput']>0.0 ))
	loss_share_est =total_branch_losses /max (total_generated ,1e-09 )if total_generated >1e-09 else 0.0 
	expected_useful_energy =max (0.0 ,total_generated -total_branch_losses )
	if loss_share_est >0.26 :
		_add_warning ('high_network_losses','loss_share_above_empirical_safe_zone')
	if branch_losses_sorted and total_branch_losses >0.0 and (branch_losses_sorted [0 ]['losses']>0.55 *total_branch_losses ):
		_add_warning ('losses_concentrated_in_one_branch','losses_concentrated_in_one_branch')
	if branch_concentration >0.56 :
		_add_warning ('branch_flow_concentration','branch_flow_concentration')
	for branch ,mix in branch_mix .items ():
		if mix ['gen']>0 and mix ['load']>0 :
			_add_warning (f'mixed_branch:{branch }',f'mixed_branch:{branch }')
	structural_fail =bool (broken_network_rows or rootless_network_routes or cyclic_network_routes or duplicate_network_routes or conflicting_network_routes or missing_prefix_routes or broken_objects or islanded_objects )
	return {'branch_losses':branch_losses_sorted [:10 ],'branch_mix':branch_mix ,'warnings':warnings ,'vulnerabilities':vulnerabilities ,'branch_concentration_score':branch_concentration ,'loss_share_est':loss_share_est ,'expected_useful_energy':expected_useful_energy ,'hospital_inputs':hospital_inputs ,'factory_inputs':factory_inputs ,'is_tree_like':not structural_fail ,'avg_object_path_depth':avg (object_path_depths ,default =0.0 )}

def compute_reserve (state ,future ,object_rows ,useful_supply_now ,profile_ctx =None ):
	gen_total =sum ((r ['generated']for r in object_rows ))
	wind_gen =sum ((r ['generated']for r in object_rows if r ['type']=='wind'))
	wind_share =wind_gen /max (gen_total ,1e-09 )if gen_total >0 else 0.0 
	abs_err =sf (state .get ('abs_err_ewma',1.2 ),1.2 )
	loss_ratio =sf (state .get ('loss_ratio_ewma',0.18 ),0.18 )
	next_useful =sf (future [0 ].get ('useful_supply_pred',0.0 ),0.0 )if future else useful_supply_now 
	next_balance =sf (future [0 ].get ('balance_pred',0.0 ),0.0 )if future else 0.0 
	reserve =0.18 +0.28 *abs_err +0.08 *max (0.0 ,loss_ratio -0.1 )*max (gen_total ,useful_supply_now )
	reserve +=0.06 *wind_share *max (wind_gen ,next_useful )
	if next_useful <useful_supply_now :
		reserve +=0.14 *(useful_supply_now -next_useful )
	if next_balance <-2.0 :
		reserve +=0.04 *abs (next_balance )
	if profile_ctx :
		current =profile_ctx .get ('current',{})
		avg_combo =sf (profile_ctx .get ('avg_combo_12',0.0 ),0.0 )
		avg_risk =sf (profile_ctx .get ('avg_risk_12',0.0 ),0.0 )
		if current .get ('mixed_peak')or avg_combo >avg_risk +0.1 :
			reserve *=0.88 
		if current .get ('protect_bias',0.0 )>0.5 or avg_risk >avg_combo +0.1 :
			reserve *=1.1 
	if useful_supply_now >0.0 :
		reserve =min (reserve ,max (MIN_RESERVE ,0.18 *useful_supply_now ))
	return max (MIN_RESERVE ,reserve )

def build_ladder (sell_volume ,market_ref ,fill_ratio ,max_tickets ,cfg ,buy_ref =None ,profile_ctx =None ,market_ctx =None ,startup_mode =False ,storage_excess =False ):
	if sell_volume <MOV :
		return []
	market_cap =cl (sf (cfg .get ('exchangeExternalBuy',PMX ),PMX ),PMN ,PMX )
	gp_price =cl (sf (cfg .get ('exchangeExternalSell',PMN ),PMN ),PMN ,market_cap )
	step =sf (cfg .get ('exchangeConsumerPriceStep',0.2 ),0.2 )
	max_tickets =max (0 ,si (max_tickets ,0 ))
	buy_ref =sf (buy_ref ,None )
	current =profile_ctx .get ('current',{})if profile_ctx else {}
	avg_combo =sf (profile_ctx .get ('avg_combo_12',0.0 ),0.0 )if profile_ctx else 0.0 
	avg_risk =sf (profile_ctx .get ('avg_risk_12',0.0 ),0.0 )if profile_ctx else 0.0 
	price_realism =sf (market_ctx .get ('price_realism',1.0 ),1.0 )if market_ctx else 1.0 
	forecast_oversupply =cl (sf (market_ctx .get ('forecast_oversupply',0.5 ),0.5 ),0.0 ,1.0 )if market_ctx else 0.5 
	forecast_tightness =cl (sf (market_ctx .get ('forecast_tightness',0.5 ),0.5 ),0.0 ,1.0 )if market_ctx else 0.5 
	competition_pressure =cl (sf (market_ctx .get ('competition_pressure',0.0 ),0.0 ),-1.0 ,1.0 )if market_ctx else 0.0 
	competition_buy_ref =sf (market_ctx .get ('competition_buy_ref',None ),None )if market_ctx else None 
	competition_sell_ref =sf (market_ctx .get ('competition_sell_ref',None ),None )if market_ctx else None 
	competition_strong_buy =bool (market_ctx .get ('competition_strong_buy',False ))if market_ctx else False 
	competition_strong_sell =bool (market_ctx .get ('competition_strong_sell',False ))if market_ctx else False 
	overpriced =bool (market_ctx .get ('overpriced',False ))if market_ctx else False 
	underpriced =bool (market_ctx .get ('underpriced',False ))if market_ctx else False 
	underpriced_boost =cl (sf (market_ctx .get ('underpriced_boost_steps',0.0 ),0.0 ),0.0 ,1.2 )if market_ctx else 0.0 
	persistent_underpriced =bool (market_ctx .get ('persistent_underpriced',False ))if market_ctx else False 
	near_zero_fill =bool (market_ctx .get ('near_zero_fill',False ))if market_ctx else False 
	good_fill =bool (market_ctx .get ('good_fill',False ))if market_ctx else False 
	weak_fill =bool (market_ctx .get ('weak_fill',False ))if market_ctx else fill_ratio <sf (MI .get ('weak_fill_ratio',0.78 ),0.78 )
	high_volume_soft_cap =sf (MI .get ('high_volume_soft_cap',8.0 ),8.0 )
	bad_fill_ratio =sf (MI .get ('bad_fill_ratio',0.58 ),0.58 )
	good_fill_ratio =sf (MI .get ('good_fill_ratio',0.88 ),0.88 )
	pricing_bias_steps =sf (market_ctx .get ('pricing_bias_steps',0.0 ),0.0 )if market_ctx else 0.0 
	anti_dump_cap =max (sell_volume ,sf (market_ctx .get ('anti_dump_cap',sell_volume ),sell_volume ),1.0 )if market_ctx else max (sell_volume ,1.0 )
	volume_share =cl (sell_volume /anti_dump_cap ,0.0 ,1.8 )
	volume_pressure =cl ((sell_volume -0.55 *high_volume_soft_cap )/max (high_volume_soft_cap ,1.0 ),-0.5 ,2.2 )
	recent_execution =sf (market_ctx .get ('recent_execution_ratio',1.0 ),1.0 )if market_ctx else 1.0 
	has_execution_history =bool (market_ctx .get ('has_execution_history',False ))if market_ctx else False 
	good_execution_ratio =sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
	if sell_volume <=0.42 *high_volume_soft_cap and volume_share <=0.55 and (not weak_fill )and (not overpriced )and (not near_zero_fill ):
		volume_mode ='small_volume_premium'
	elif sell_volume >=1.2 *high_volume_soft_cap or volume_share >=0.95 or near_zero_fill or (weak_fill and sell_volume >=0.7 *high_volume_soft_cap ):
		volume_mode ='forced_liquidity'
	else :
		volume_mode ='normal_bulk'
	strong_market_confirmation =bool (good_fill and has_execution_history and (recent_execution >=good_execution_ratio )and (price_realism >=0.9 )and (not weak_fill )and (not overpriced )and (not near_zero_fill ))
	market_anchor =sf (market_ctx .get ('market_ref',market_ref ),market_ref )if market_ctx else market_ref 
	opp_anchor =sf (market_ctx .get ('market_rolling_ref',market_anchor ),market_anchor )if market_ctx else market_anchor 
	base_ref =market_anchor 
	if buy_ref is not None and (not weak_fill )and (not overpriced )and (price_realism >=0.82 ):
		buy_anchor =cl (buy_ref ,market_anchor -2.0 *step ,market_anchor +1.2 *step )
		base_ref =0.9 *base_ref +0.1 *buy_anchor 
	if competition_buy_ref is not None and (not weak_fill or competition_pressure >0.1 ):
		comp_anchor =cl (competition_buy_ref +0.15 *competition_pressure *step ,market_anchor -1.6 *step ,market_anchor +1.4 *step )
		base_ref =0.86 *base_ref +0.14 *comp_anchor 
	if competition_sell_ref is not None and (competition_pressure <-0.08 or competition_strong_sell ):
		base_ref =min (base_ref ,competition_sell_ref -(0.22 +0.4 *abs (min (0.0 ,competition_pressure )))*step )
	if fill_ratio <bad_fill_ratio :
		base_ref -=0.9 *step 
	elif fill_ratio >good_fill_ratio and (not overpriced ):
		base_ref +=0.35 *step 
	if current .get ('mixed_peak')or avg_combo >avg_risk +0.1 :
		base_ref +=0.35 *step 
	if current .get ('protect_bias',0.0 )>0.5 or avg_risk >avg_combo +0.15 :
		base_ref -=0.4 *step 
	if startup_mode :
		base_ref -=0.4 *step 
	if storage_excess :
		base_ref -=0.3 *step 
	if forecast_oversupply >0.62 :
		base_ref -=(0.16 +0.36 *(forecast_oversupply -0.62 ))*step 
	if forecast_tightness >0.62 and (not weak_fill ):
		base_ref +=(0.08 +0.26 *(forecast_tightness -0.62 ))*step 
	if competition_strong_buy and (not weak_fill )and (not overpriced ):
		base_ref +=0.12 *step 
	if competition_strong_sell :
		base_ref -=0.24 *step 
	if weak_fill or near_zero_fill :
		base_ref =min (base_ref ,opp_anchor -0.55 *step )
	elif price_realism <0.8 and (not underpriced ):
		base_ref =min (base_ref ,opp_anchor -0.25 *step )
	if underpriced and good_fill and (not weak_fill )and (not near_zero_fill ):
		base_ref +=(0.2 +0.12 *underpriced_boost )*step 
		if persistent_underpriced :
			base_ref +=(0.1 +0.1 *underpriced_boost )*step 
	base_ref =cl (base_ref ,PMN ,market_cap )
	bulk_delta =0.35 -1.05 *volume_pressure +pricing_bias_steps 
	if volume_mode =='small_volume_premium':
		bulk_delta +=0.3 
	elif volume_mode =='forced_liquidity':
		bulk_delta -=0.65 
	if weak_fill :
		bulk_delta -=0.65 
	if overpriced :
		bulk_delta -=0.75 
	if near_zero_fill :
		bulk_delta -=1.1 
	if underpriced and good_fill :
		bulk_delta +=0.24 +0.2 *underpriced_boost 
	if persistent_underpriced and (not weak_fill )and (not overpriced )and (not near_zero_fill ):
		bulk_delta +=0.12 +0.16 *underpriced_boost 
	if competition_pressure <-0.2 :
		bulk_delta -=0.22 +0.36 *abs (competition_pressure )
	elif competition_pressure >0.2 and (not weak_fill )and (not overpriced ):
		bulk_delta +=0.14 +0.28 *competition_pressure 
	if forecast_oversupply >0.68 :
		bulk_delta -=0.22 +0.3 *(forecast_oversupply -0.68 )
	if forecast_tightness >0.68 and (not weak_fill ):
		bulk_delta +=0.1 +0.24 *(forecast_tightness -0.68 )
	if volume_share >0.95 :
		bulk_delta -=0.45 
	if volume_mode =='small_volume_premium':
		bulk_up_cap =1.8 
	elif volume_mode =='forced_liquidity':
		bulk_up_cap =0.2 
	else :
		bulk_up_cap =0.9 
	if weak_fill or overpriced :
		bulk_up_cap =min (bulk_up_cap ,0.1 )
	bulk_delta =cl (bulk_delta ,-2.8 ,bulk_up_cap )
	if volume_mode =='small_volume_premium':
		mid_delta =bulk_delta +1.25 
		tail_delta =mid_delta +1.55 
	elif volume_mode =='forced_liquidity':
		mid_delta =bulk_delta +0.55 
		tail_delta =mid_delta +0.55 
	else :
		mid_delta =bulk_delta +0.85 
		tail_delta =mid_delta +1.05 
	if weak_fill or near_zero_fill :
		mid_delta =min (mid_delta ,bulk_delta +0.7 )
		tail_delta =min (tail_delta ,mid_delta +0.7 )
	if underpriced and good_fill :
		mid_delta +=0.14 +0.24 *underpriced_boost 
	if persistent_underpriced and underpriced and good_fill :
		mid_delta +=0.12 +0.2 *underpriced_boost 
		if sell_volume <=0.55 *high_volume_soft_cap :
			tail_delta +=0.18 +0.18 *underpriced_boost 
	elif underpriced and good_fill and (sell_volume <=0.45 *high_volume_soft_cap ):
		tail_delta +=0.35 
	floor_price =max (PMN ,gp_price +step )
	bulk_price =base_ref +bulk_delta *step 
	mid_price =base_ref +mid_delta *step 
	tail_price =base_ref +tail_delta *step 
	bulk_guard_high =market_anchor +(0.8 if sell_volume >high_volume_soft_cap else 1.4 )*step 
	if sell_volume >high_volume_soft_cap and (not strong_market_confirmation ):
		bulk_guard_high =min (bulk_guard_high ,market_anchor +step )
	if volume_mode =='forced_liquidity':
		bulk_guard_high =min (bulk_guard_high ,market_anchor +(step if strong_market_confirmation else 0.4 *step ))
	if weak_fill or overpriced :
		bulk_guard_high =min (bulk_guard_high ,market_anchor +0.6 *step )
	bulk_price =cl (bulk_price ,floor_price ,min (market_cap ,bulk_guard_high ))
	mid_guard_high =market_anchor +(1.6 if sell_volume >high_volume_soft_cap else 2.4 )*step 
	tail_guard_high =market_anchor +(2.2 if sell_volume <=0.45 *high_volume_soft_cap else 1.6 if sell_volume <=high_volume_soft_cap else 1.2 )*step 
	if weak_fill or near_zero_fill :
		mid_guard_high =min (mid_guard_high ,market_anchor +1.0 *step )
		tail_guard_high =min (tail_guard_high ,market_anchor +1.4 *step )
	mid_price =cl (mid_price ,min (market_cap ,bulk_price +step ),min (market_cap ,max (bulk_price +step ,mid_guard_high )))
	tail_price =cl (tail_price ,min (market_cap ,mid_price +step ),min (market_cap ,max (mid_price +step ,tail_guard_high )))
	if startup_mode :
		prices =[bulk_price ,mid_price ]
		shares =[0.9 ,0.1 ]
	elif near_zero_fill or (weak_fill and overpriced ):
		prices =[bulk_price ,mid_price ]
		shares =[0.92 ,0.08 ]
	elif weak_fill or overpriced :
		prices =[bulk_price ,mid_price ,tail_price ]
		shares =[0.84 ,0.13 ,0.03 ]
	elif volume_mode =='small_volume_premium':
		prices =[bulk_price ,mid_price ,tail_price ]
		shares =[0.5 ,0.3 ,0.2 ]
	elif volume_mode =='forced_liquidity':
		prices =[bulk_price ,mid_price ,tail_price ]
		shares =[0.9 ,0.09 ,0.01 ]
	else :
		prices =[bulk_price ,mid_price ,tail_price ]
		shares =[0.74 ,0.2 ,0.06 ]
	if underpriced and good_fill and (len (shares )==3 )and (sell_volume <=0.45 *high_volume_soft_cap ):
		shares =[0.56 ,0.27 ,0.17 ]
	shares =[s /max (1e-09 ,sum (shares ))for s in shares ]
	prices =[round_price (p ,price_max =market_cap ,price_step =step )for p in prices ]
	for i in range (1 ,len (prices )):
		if prices [i ]<prices [i -1 ]:
			prices [i ]=prices [i -1 ]
	out =[]
	allocated =0.0 
	for i ,share in enumerate (shares ):
		if i ==len (shares )-1 :
			vol =max (0.0 ,sell_volume -allocated )
		else :
			vol =round_vol (sell_volume *share )
			allocated +=vol 
		if vol >=MOV :
			out .append ((round_vol (vol ),prices [i ]))
	return out [:max_tickets ]

def compute_safe_sell_volume (state ,object_rows ,marketable_useful_now ,offer_cap ,reserve ,balance_now ,topology ,market_ctx ):
	gen_total =sum ((r ['generated']for r in object_rows ))
	wind_total =sum ((r ['generated']for r in object_rows if r .get ('type')=='wind'))
	wind_share =wind_total /max (gen_total ,1e-09 )if gen_total >0.0 else 0.0 
	loss_ratio =sf (state .get ('loss_ratio_ewma',0.18 ),0.18 )
	abs_err =sf (state .get ('abs_err_ewma',1.2 ),1.2 )
	recent_fill =sf (market_ctx .get ('recent_fill_ratio',sf (state .get ('fill_ratio_ewma',0.84 ),0.84 )),0.84 )
	recent_execution =sf (market_ctx .get ('recent_execution_ratio',sf (state .get ('execution_ratio_ewma',0.72 ),0.72 )),0.72 )
	execution_signal =sf (market_ctx .get ('execution_signal_ratio',recent_execution ),recent_execution )
	price_realism =sf (market_ctx .get ('price_realism',1.0 ),1.0 )
	has_fill_history =bool (market_ctx .get ('has_fill_history',False ))
	has_execution_history =bool (market_ctx .get ('has_execution_history',False ))
	weak_execution =bool (market_ctx .get ('weak_execution',False ))
	competition_pressure =cl (sf (market_ctx .get ('competition_pressure',0.0 ),0.0 ),-1.0 ,1.0 )
	competition_strong_buy =bool (market_ctx .get ('competition_strong_buy',False ))
	competition_strong_sell =bool (market_ctx .get ('competition_strong_sell',False ))
	forecast_oversupply =cl (sf (market_ctx .get ('forecast_oversupply',0.5 ),0.5 ),0.0 ,1.0 )
	forecast_tightness =cl (sf (market_ctx .get ('forecast_tightness',0.5 ),0.5 ),0.0 ,1.0 )
	forecast_balance_avg =sf (market_ctx .get ('forecast_balance_avg',0.0 ),0.0 )
	overpriced =bool (market_ctx .get ('overpriced',False ))
	underpriced =bool (market_ctx .get ('underpriced',False ))
	near_zero_fill =bool (market_ctx .get ('near_zero_fill',False ))
	near_zero_execution =bool (market_ctx .get ('near_zero_execution',False ))
	execution_ewma =cl (sf (state .get ('execution_ratio_ewma',execution_signal ),execution_signal ),0.0 ,1.0 )
	poor_execution_ratio =sf (MI .get ('poor_execution_ratio',0.35 ),0.35 )
	good_execution_ratio =sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
	near_zero_execution_ratio =sf (MI .get ('near_zero_execution_ratio',sf (MI .get ('near_zero_fill_ratio',0.12 ),0.12 )),sf (MI .get ('near_zero_fill_ratio',0.12 ),0.12 ))
	market_ref =sf (market_ctx .get ('market_ref',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 ))
	recent_ask =sf (market_ctx .get ('recent_ask_price',market_ref ),market_ref )
	market_step =max (0.01 ,sf (market_ctx .get ('market_price_step',0.2 ),0.2 ))
	ask_gap_steps =(recent_ask -market_ref )/market_step 
	uncertainty =0.22 *abs_err +0.12 *wind_share *marketable_useful_now 
	uncertainty +=0.1 *max (0.0 ,loss_ratio -0.12 )*marketable_useful_now 
	if 'high_network_losses'in topology .get ('warnings',[]):
		uncertainty +=0.08 *marketable_useful_now 
	safe_target =max (0.0 ,min (offer_cap ,marketable_useful_now )-max (reserve ,uncertainty ))
	if has_fill_history and recent_fill <sf (MI .get ('weak_fill_ratio',0.78 ),0.78 ):
		safe_target *=cl (0.45 +0.7 *recent_fill ,0.3 ,0.95 )
	if (has_execution_history or execution_ewma <good_execution_ratio -0.02 )and execution_signal <good_execution_ratio :
		safe_target *=cl (0.28 +0.98 *execution_signal ,0.2 ,0.92 )
	if marketable_useful_now >=MOV and has_execution_history and (weak_execution or execution_signal <poor_execution_ratio ):
		safe_target *=cl (0.42 +0.8 *execution_signal /max (poor_execution_ratio ,1e-06 ),0.18 ,0.78 )
	if marketable_useful_now >=MOV and competition_pressure <-0.2 :
		safe_target *=cl (1.0 +0.48 *competition_pressure ,0.58 ,0.96 )
	elif marketable_useful_now >=MOV and competition_pressure >0.22 and (not weak_execution ):
		safe_target *=cl (1.0 +0.14 *competition_pressure ,1.0 ,1.08 )
	if marketable_useful_now >=MOV and forecast_oversupply >0.64 and (not overpriced )and (price_realism >=0.72 ):
		safe_target *=cl (1.0 +0.14 *(forecast_oversupply -0.64 ),1.0 ,1.08 )
	if marketable_useful_now >=MOV and forecast_tightness >0.72 and (forecast_balance_avg <0.0 or balance_now <0.0 or 'high_network_losses'in topology .get ('warnings',[])):
		safe_target *=cl (1.0 -0.28 *(forecast_tightness -0.72 ),0.84 ,1.0 )
	if competition_strong_sell and marketable_useful_now >=MOV :
		safe_target *=0.92 
	if competition_strong_buy and marketable_useful_now >=MOV and (not weak_execution ):
		safe_target *=1.04 
	if price_realism <0.7 :
		safe_target *=cl (price_realism ,0.25 ,1.0 )
	if overpriced :
		safe_target *=cl (0.66 -0.07 *max (0.0 ,ask_gap_steps ),0.34 ,0.84 )
	if near_zero_fill :
		safe_target *=0.55 
	if marketable_useful_now >=MOV and (near_zero_execution or (has_execution_history and execution_signal <=near_zero_execution_ratio )):
		safe_target *=0.34 
	if underpriced and recent_fill >=sf (MI .get ('good_fill_ratio',0.88 ),0.88 )and (price_realism >=0.8 )and (not overpriced ):
		safe_target *=1.08 
	if balance_now <0.0 :
		safe_target =min (safe_target ,max (0.0 ,balance_now +marketable_useful_now ))
	safe_target =round_vol (max (0.0 ,min (offer_cap ,marketable_useful_now ,safe_target )))
	return safe_target if safe_target >=MOV else 0.0 

def normalize_cfg (raw ):

	def g (name ,default ):
		return sf (raw .get (name ,default ),default )
	return {'exchangeMaxTickets':si (raw .get ('exchangeMaxTickets',100 ),100 ),'exchangeExternalSell':g ('exchangeExternalSell',2.0 ),'exchangeExternalBuy':g ('exchangeExternalBuy',20.0 ),'exchangeExternalInstantSell':g ('exchangeExternalInstantSell',1.5 ),'exchangeExternalInstantBuy':g ('exchangeExternalInstantBuy',raw .get ('exchangeExternalIntantBuy',25.0 )),'exchangeAmountScaler':g ('exchangeAmountScaler',1.2 ),'exchangeAmountBuffer':g ('exchangeAmountBuffer',10.0 ),'cellCapacity':g ('cellCapacity',120.0 ),'cellChargeRate':g ('cellChargeRate',15.0 ),'cellDischargeRate':g ('cellDischargeRate',20.0 ),'corridorSun':g ('corridorSun',0.5 ),'corridorWind':g ('corridorWind',0.5 ),'corridorFactory':g ('corridorFactory',0.5 ),'corridorOffice':g ('corridorOffice',0.5 ),'corridorHospital':g ('corridorHospital',0.25 ),'corridorHouseA':g ('corridorHouseA',0.5 ),'corridorHouseB':g ('corridorHouseB',0.5 ),'maxSolarPower':g ('maxSolarPower',20.0 ),'maxWindPower':g ('maxWindPower',20.0 ),'exchangeConsumerPriceStep':g ('exchangeConsumerPriceStep',0.2 ),'weatherEffectsDelay':si (raw .get ('weatherEffectsDelay',0 ),0 ),'weatherMaxWind':g ('weatherMaxWind',15.0 )}

def controller (psm ):
	state =load_state ()
	tick =get_tick (psm )
	if tick ==0 :
		state =default_state ()
	game_length =get_game_length (psm )
	cfg =normalize_cfg (get_config_dict (psm ))
	object_rows =extract_object_rows (psm )
	network_rows =extract_network_rows (psm )
	exchange_rows =[exchange_receipt_data (x )for x in get_exchange_list (psm )]
	total_generated ,total_consumed ,_ ,total_losses =get_total_power_tuple (psm )
	obj_agg =aggregate_objects (object_rows )
	topology =analyze_topology (object_rows ,network_rows ,total_generated )
	weather ={'wind':get_weather_now (psm ,'wind'),'sun':get_weather_now (psm ,'sun')}
	forecast_bundle =get_forecast_bundle (psm ,game_length =game_length ,cfg =cfg ,object_rows =object_rows )
	state ['cfg_runtime']=cfg 
	state ['weather_runtime']=weather 
	refresh_static_runtime_context (state ,object_rows )
	apply_startup_observation (state ,object_rows ,forecast_bundle ,tick ,total_consumed =total_consumed )
	forecast_profile =build_forecast_profile (state ,forecast_bundle ,object_rows ,game_length )
	profile_ctx =forecast_profile_context (forecast_profile ,tick ,horizon =max (12 ,LOOKAHEAD *2 ))
	future =forecast_window (state ,object_rows ,forecast_bundle ,tick ,game_length ,LOOKAHEAD )
	useful_raw =compute_useful_energy (total_generated ,total_losses )
	useful_now =max (0.0 ,useful_raw )
	balance_now =compute_balance_energy (total_generated ,total_consumed ,total_losses )
	market_stats =analyze_exchange (exchange_rows )
	prev_sell_order =sf (state .get ('last_sell_volume',0.0 ),0.0 )
	sell_asked_now =sf (market_stats .get ('sell_asked',0.0 ),0.0 )
	sell_contracted_now =sf (market_stats .get ('sell_contracted',0.0 ),0.0 )
	execution_ratio_now =None 
	if prev_sell_order >=MOV :
		execution_ratio_now =cl (sell_contracted_now /max (prev_sell_order ,1e-09 ),0.0 ,1.0 )
		state ['execution_ratio_ewma']=0.7 *sf (state .get ('execution_ratio_ewma',0.72 ),0.72 )+0.3 *execution_ratio_now 
	buy_ref =market_stats .get ('buy_avg_contracted_price')or market_stats .get ('buy_avg_asked_price')
	fill_ratio_now =market_stats .get ('sell_fill_ratio')
	exch_log =get_exchange_log (psm )
	market_step =max (0.01 ,sf (cfg .get ('exchangeConsumerPriceStep',0.2 ),0.2 ))
	market_floor =cl (sf (cfg .get ('exchangeExternalSell',PMN ),PMN ),PMN ,PMX )
	market_cap_cfg =cl (sf (cfg .get ('exchangeExternalBuy',PMX ),PMX ),market_floor +market_step ,PMX )
	market_log_window =max (4 ,si (MI .get ('market_log_window',12 ),12 ))
	exchange_prices =extract_exchange_log_prices (exch_log ,limit =max (8 ,market_log_window *2 ))
	exchange_reports =get_exchange_reports (psm )
	exchange_tickets =get_exchange_tickets (psm )
	competition_feed =list (exchange_reports )+list (exchange_tickets )+list (exch_log )[-max (6 ,market_log_window ):]
	competition_book =summarize_competition_book (competition_feed ,market_floor +market_step ,market_cap_cfg )
	update_competition_history (state ,tick ,competition_book )
	state ['exchange_price_history']=exchange_prices 
	market_seed =summarize_market_prices (exchange_prices [-market_log_window :],sf (state .get ('ewma_market_price',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 )),market_floor +market_step ,market_cap_cfg ,alpha =sf (MI .get ('market_log_ewma_alpha',0.34 ),0.34 ))
	fallback_ref =sf (state .get ('market_ref',4.8 ),4.8 )
	state ['last_market_price']=sf (market_seed .get ('last',fallback_ref ),fallback_ref )
	state ['ewma_market_price']=sf (market_seed .get ('ewma',fallback_ref ),fallback_ref )
	state ['median_market_price']=sf (market_seed .get ('median',fallback_ref ),fallback_ref )
	state ['conservative_market_price']=sf (market_seed .get ('conservative',fallback_ref ),fallback_ref )
	prev_ref =sf (state .get ('market_ref',4.8 ),4.8 )
	if si (market_seed .get ('count',0 ),0 )>0 :
		market_anchor =sf (market_seed .get ('reference',prev_ref ),prev_ref )
		state ['market_ref']=0.2 *prev_ref +0.8 *market_anchor 
	else :
		state ['market_ref']=0.92 *prev_ref +0.08 *sf (state .get ('ewma_market_price',prev_ref ),prev_ref )
	if fill_ratio_now is not None :
		state ['fill_ratio_ewma']=0.76 *sf (state .get ('fill_ratio_ewma',0.84 ),0.84 )+0.24 *fill_ratio_now 
	bias_steps =sf (state .get ('sell_bias_steps',0.0 ),0.0 )
	if execution_ratio_now is not None :
		fill_eval_fallback =0.0 if sell_asked_now <MOV else cl (sell_contracted_now /max (sell_asked_now ,1e-09 ),0.0 ,1.0 )
		fill_eval =sf (fill_ratio_now ,fill_eval_fallback )
		score =0.55 *cl (execution_ratio_now ,0.0 ,1.2 )+0.45 *cl (fill_eval ,0.0 ,1.0 )
		if score <0.45 :
			bias_steps -=0.12 
		elif score <0.7 :
			bias_steps -=0.06 
		elif score >0.92 :
			bias_steps +=0.03 
		elif score >0.82 :
			bias_steps +=0.015 
	state ['sell_bias_steps']=cl (0.96 *bias_steps ,-2.8 ,1.2 )
	update_market_history (state ,tick ,market_stats ,execution_ratio =execution_ratio_now )
	market_ctx =build_market_context (state )
	market_ctx .update (forecast_market_pressure (future ,cfg ,profile_ctx =profile_ctx ))
	observed_gap_steps =max (0.0 ,(sf (market_ctx .get ('market_ref',0.0 ),0.0 )-sf (market_ctx .get ('recent_ask_price',0.0 ),0.0 ))/max (market_step ,1e-06 ))
	execution_good =not market_ctx .get ('has_execution_history')or sf (market_ctx .get ('recent_execution_ratio',0.0 ),0.0 )>=sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
	underpriced_easy =bool (market_ctx .get ('underpriced')and market_ctx .get ('good_fill')and execution_good and (observed_gap_steps >=1.0 ))
	underpriced_streak =max (0 ,si (state .get ('underpriced_streak',0 ),0 ))
	underpriced_boost_steps =cl (sf (state .get ('underpriced_boost_steps',0.0 ),0.0 ),0.0 ,1.2 )
	if underpriced_easy :
		underpriced_streak =min (6 ,underpriced_streak +1 )
	elif bool (market_ctx .get ('underpriced',False )):
		underpriced_streak =max (0 ,underpriced_streak -1 )
	else :
		underpriced_streak =max (0 ,underpriced_streak -2 )
	if underpriced_easy and underpriced_streak >=2 :
		underpriced_boost_steps =cl (underpriced_boost_steps +0.1 +0.04 *min (3.0 ,observed_gap_steps ),0.0 ,1.2 )
	elif bool (market_ctx .get ('overpriced',False ))or bool (market_ctx .get ('weak_fill',False ))or bool (market_ctx .get ('near_zero_fill',False )):
		underpriced_boost_steps =cl (underpriced_boost_steps -0.22 ,0.0 ,1.2 )
	else :
		underpriced_boost_steps =cl (underpriced_boost_steps -0.08 ,0.0 ,1.2 )
	state ['underpriced_streak']=underpriced_streak 
	state ['underpriced_boost_steps']=underpriced_boost_steps 
	market_ctx ['persistent_underpriced']=bool (underpriced_easy and underpriced_streak >=2 )
	market_ctx ['underpriced_boost_steps']=underpriced_boost_steps 
	state ['market_ref']=sf (market_ctx .get ('market_ref',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 ))
	anti_dump_cap_preview =compute_offer_cap (state ,cfg ,tick ,useful_now )
	market_ctx ['anti_dump_cap']=anti_dump_cap_preview 
	market_ctx ['anti_dump_headroom']=max (0.0 ,anti_dump_cap_preview -prev_sell_order )
	decision_loss_ratio =sf (state .get ('loss_ratio_ewma',0.18 ),0.18 )
	charge_orders ,discharge_orders ,battery_dbg =storage_policy (state ,cfg ,obj_agg ['storages'],balance_now ,future ,sf (market_ctx .get ('recent_fill_ratio',state .get ('fill_ratio_ewma',0.84 )),0.84 ),tick ,game_length ,loss_ratio =decision_loss_ratio ,profile_ctx =profile_ctx ,market_ctx =market_ctx )
	startup_mode =startup_active (state ,tick )
	stable_surplus_now =max (0.0 ,balance_now )
	surplus_after_storage =max (0.0 ,stable_surplus_now -battery_dbg ['charge_total'])
	gross_marketable_useful_now =max (0.0 ,surplus_after_storage +battery_dbg ['discharge_for_market'])
	stress_sell_mode =bool (balance_now <0.0 or decision_loss_ratio >0.3 or profile_ctx .get ('current',{}).get ('protect_bias',0.0 )>0.5 or ('high_network_losses'in topology .get ('warnings',[])))
	marketable_useful_now =gross_marketable_useful_now 
	if stress_sell_mode :
		marketable_useful_now =min (marketable_useful_now ,max (0.0 ,balance_now +battery_dbg ['discharge_for_market']-battery_dbg ['charge_total']))
	offer_cap =compute_offer_cap (state ,cfg ,tick ,marketable_useful_now )
	reserve =compute_reserve (state ,future ,object_rows ,marketable_useful_now ,profile_ctx =profile_ctx )
	market_ctx ['anti_dump_cap']=offer_cap 
	market_ctx ['anti_dump_headroom']=max (0.0 ,offer_cap -prev_sell_order )
	sell_volume =compute_safe_sell_volume (state ,object_rows ,marketable_useful_now ,offer_cap ,reserve ,balance_now ,topology ,market_ctx )
	storage_sell_guard_gap =max (MOV ,0.006 *len (obj_agg ['storages'])*sf (cfg ['cellCapacity'],120.0 ))
	premium_sell_ready =bool (battery_dbg .get ('premium_sell_ready',False ))
	prep_soc_now =sf (battery_dbg .get ('prep_soc',battery_dbg .get ('target_soc',0.0 )),sf (battery_dbg .get ('target_soc',0.0 ),0.0 ))
	total_soc_now =sf (battery_dbg .get ('total_soc',0.0 ),0.0 )
	physical_storage_room_after_orders =max (0.0 ,sf (battery_dbg .get ('soc_ceil',0.0 ),0.0 )-(total_soc_now +sf (battery_dbg .get ('charge_total',0.0 ),0.0 )))
	storage_needs_capture =bool (prep_soc_now >total_soc_now +storage_sell_guard_gap or battery_dbg .get ('fill_to_ceiling',False ))
	storage_is_nearly_full =physical_storage_room_after_orders <=storage_sell_guard_gap 
	recent_ask_price =sf (market_ctx .get ('recent_ask_price',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 ))
	market_hot_ref =sf (market_ctx .get ('market_rolling_ref',market_ctx .get ('market_ref',state .get ('market_ref',4.8 ))),sf (state .get ('market_ref',4.8 ),4.8 ))
	hot_price_sell_ready =bool (premium_sell_ready or (sf (market_ctx .get ('price_realism',1.0 ),1.0 )>=0.94 and recent_ask_price >=market_hot_ref +0.8 *market_step ))
	sell_blocked_for_storage =bool (stable_surplus_now >=MOV and storage_needs_capture and (physical_storage_room_after_orders >storage_sell_guard_gap )and (battery_dbg .get ('charge_total',0.0 )<=0.0 )and (not hot_price_sell_ready ))
	if sell_volume >0.0 and sell_blocked_for_storage :
		sell_volume =0.0 
	safe_sell_before_fallback =sell_volume 
	recent_execution_ratio =sf (market_ctx .get ('recent_execution_ratio',sf (state .get ('execution_ratio_ewma',0.72 ),0.72 )),sf (state .get ('execution_ratio_ewma',0.72 ),0.72 ))
	good_execution_ratio =sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
	has_execution_history =bool (market_ctx .get ('has_execution_history',False ))
	execution_history_good =has_execution_history and recent_execution_ratio >=good_execution_ratio 
	observed_market_good =bool (market_ctx .get ('good_fill')and execution_history_good and (sf (market_ctx .get ('price_realism',1.0 ),1.0 )>=0.86 )and (not bool (market_ctx .get ('near_zero_fill',False ))))
	neutral_safe_sell_volume =0.0 
	if safe_sell_before_fallback <=0.0 and marketable_useful_now >=MOV :
		neutral_market_ctx =dict (market_ctx )
		neutral_good_execution =sf (MI .get ('good_execution_ratio',0.72 ),0.72 )
		neutral_market_ctx .update ({'price_realism':1.0 ,'overpriced':False ,'underpriced':False ,'persistent_underpriced':False ,'underpriced_boost_steps':0.0 ,'near_zero_fill':False ,'near_zero_execution':False ,'weak_execution':False ,'has_fill_history':False ,'has_execution_history':False ,'recent_execution_ratio':neutral_good_execution ,'execution_signal_ratio':neutral_good_execution ,'competition_pressure':0.0 ,'competition_strong_buy':False ,'competition_strong_sell':False ,'forecast_oversupply':0.5 ,'forecast_tightness':0.5 ,'forecast_balance_avg':0.0 })
		neutral_safe_sell_volume =compute_safe_sell_volume (state ,object_rows ,marketable_useful_now ,offer_cap ,reserve ,balance_now ,topology ,neutral_market_ctx )
	safe_zero_due_market_caution =safe_sell_before_fallback <=0.0 and neutral_safe_sell_volume >=MOV and (balance_now >=0.0 )
	fallback_sell_ready =bool (observed_market_good and (not bool (market_ctx .get ('overpriced',False )))and storage_is_nearly_full and safe_zero_due_market_caution and (not sell_blocked_for_storage ))
	if startup_mode and battery_dbg .get ('signal',0.0 )<=reserve +0.5 :
		sell_volume =0.0 
	elif marketable_useful_now >=MOV and sell_volume <=0.0 and fallback_sell_ready :
		sell_volume =round_vol (min (neutral_safe_sell_volume ,offer_cap ,marketable_useful_now ,max (MOV ,0.48 *marketable_useful_now )))
	storage_excess =battery_dbg ['total_soc']>battery_dbg .get ('prep_soc',battery_dbg ['target_soc'])+0.08 *len (obj_agg ['storages'])*sf (cfg ['cellCapacity'],120.0 )
	ladder =build_ladder (sell_volume ,sf (market_ctx .get ('market_ref',state .get ('market_ref',4.8 )),sf (state .get ('market_ref',4.8 ),4.8 )),sf (market_ctx .get ('recent_fill_ratio',state .get ('fill_ratio_ewma',0.84 )),0.84 ),si (cfg ['exchangeMaxTickets'],100 ),cfg ,buy_ref =buy_ref ,profile_ctx =profile_ctx ,market_ctx =market_ctx ,startup_mode =startup_mode ,storage_excess =storage_excess )
	if hasattr (psm ,'orders'):
		for sid ,amount in charge_orders :
			if amount >0.0 :
				psm .orders .charge (sid ,amount )
		for sid ,amount in discharge_orders :
			if amount >0.0 :
				psm .orders .discharge (sid ,amount )
		for volume ,price in ladder :
			psm .orders .sell (volume ,price )
	summary_row ={'tick':tick ,'sell_volume':round (sell_volume ,6 ),'ladder':ladder }
	apply_post_tick_learning (state ,object_rows ,weather ,forecast_bundle ,tick ,total_consumed =total_consumed ,total_losses =total_losses ,marketable_useful_now =marketable_useful_now ,total_generated =total_generated )
	state ['prev_useful_supply_est']=marketable_useful_now 
	state ['prev_useful_energy_actual']=useful_now 
	state ['last_sell_volume']=sell_volume 
	save_state (state )
	return summary_row 

def main ():
	psm =ips .init ()
	try :
		summary =controller (psm )
		print (json .dumps (summary ,ensure_ascii =False ))
	except Exception as e :
		err ={'tick':get_tick (psm ),'error':str (e )}
		print (json .dumps (err ,ensure_ascii =False ))
	if hasattr (psm ,'save_and_exit'):
		psm .save_and_exit ()
if __name__ =='__main__':
	main ()
