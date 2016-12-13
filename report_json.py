{
   "reporttype":   "historic",  		 	# live|historic live returns the data from redis and historic from dynamo                                 
   "datefrom"  :   20161213123455 ,  		# int date with the "%Y%m%d%H%M%S" format (if not set no date sort will be used)
   "dateto"	   :   20161214123455,  		# int date with the "%Y%m%d%H%M%S" format
   "entitytype":   "account",        		# account|trunk|phonenumber   type of entity you want the report for
   "entityid"  :   "ACC-1230" ,       		# the id of chosen entity
   "filterby"  :   [{"calltype":"mobile"}], # list of filters which must be applied to the result set
   "callresult":   True						# True|False    indicate if the indivitual calls (call list) are required
}

