<?php
    define("BASEPATH", "SET");
    require_once"../../../config/url.php";
	require_once(base_url() .'config/config.php');

 

	
   function getRofarchiveData($request, $dateStart, $dateEnd){
		global $connect;
		$answer_last = array();  
		for ($i=0;$i<count($request);$i++){ 
			$qry = "select code, state, update_time from io_data where code=".$request[$i][0] ."and UPDATE_TIME >= '" . $dateStart . ".000' AND 
		   					UPDATE_TIME <= '" . $dateEnd . ".999'";
			
			$svg_objects_query = $connect['scada_db']->query($qry);
			$answer = array();
			while($row = $svg_objects_query->fetch(PDO::FETCH_ASSOC)){
				$answer[] = array (
							$row['code'],
							$row['update_time'],
							$row['state'],
							
							);   
			}
			$svg_objects_query = null;
			if(count($answer)!=0){
				array_push($answer_last, $answer); 
			}else{
				array_push($answer_last, array(array()));
			}
		}
		return json_encode($answer_last);
	}
	 function getTableData($request, $dateStart, $dateEnd){
		global $connect;
		
		$answerFinal = array();
		for ($i=0;$i<count($request);$i++){ 
			 
			$qry = "
										 

									 
									  
										select  min(e.update_time)as update_ti, e.day_right,e.smena, sum(e.flag_smena)as motohours, max(e.flag_smena2)as moto_prev, min(e.date_start)as date_start
											from (
											select d.code,d.state, d.update_time, d.day_right , d.hour, d.work_hours,d.smena,d.flag_smena_last_record as flag_smena, d.flag_smena2,d.date_start 
										from (
											select   c.code,c.state, c.update_time, c.day_right , c.hour, c.work_hours,c.smena,c.flag_smena, c.flag_smena2, c.flag_lastrecord, 
											case 
												when 
													lead(c.flag_lastrecord) over (order by c.update_time , c.state) ='last_record' 
														then  
															 c.flag_smena
														else c.flag_smena
											end as  flag_smena_last_record,
											case
												when 
													lead(c.flag_lastrecord) over (order by c.update_time , c.state) ='last_record' then lead(c.update_time) over (order by c.update_time , c.state) 
											end as  date_start	
										from
											(select   b.code,b.state, b.update_time, b.day_right , b.hour, b.work_hours,b.smena,b.flag_smena, b.flag_smena2,
												case 
												when 
													LAST_VALUE(b.update_time) OVER (ORDER BY b.update_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)>b.update_time  then 'no'
													else  
														case
															when b.state=1 then 'last_record'
															else 'no'
														end 

												end as flag_lastrecord
											
											from (
										select  a.code,a.state, a.update_time, a.day_right , a.hour, a.work_hours,a.smena,	 
											case 
													when a.state=0 then 
													case   
														when a.work_hours is null then 
															datediff(mi,CONVERT(datetime,CONVERT(varchar, '" . $dateStart . "' ,120)),a.update_time) 
													 
														 
															when lag(a.smena) over (order by a.update_time , a.state) <> a.smena or a.work_hours>720  then  
																case	
																	when DATEPART(hour, update_time) in (20,21,22,23) then
																		 datediff(mi,CONVERT(datetime,CONVERT(varchar, a.update_time, 23)+' '+'20:'+'00'+':'+'00' ,120),a.update_time)  
																	else
																		case
																			when 
																				DATEPART(hour, update_time) in (0,1,2,3,4,5,6,7) then
																				datediff(mi,CONVERT(datetime, convert(varchar,DATEADD(day, -1, CONVERT(varchar, a.update_time, 23)),23)+' '+'20:'+'00'+':'+'00',120),a.update_time)  
																			when DATEPART(hour, update_time) in (8,9,10,11,12,13,14,15,16,17,18,19) then	 
																				datediff(mi,CONVERT(datetime,CONVERT(varchar, a.update_time, 23)+' '+'08:'+'00'+':'+'00' ,120),a.update_time)
																		end 			
																end
															else work_hours
														end 
													else work_hours	
												end as flag_smena,
												case 
													when a.state=0 then   
														case
															when lag(a.smena) over (order by a.update_time , a.state) <> a.smena or a.work_hours>720   then
																case
																	when DATEPART(hour, update_time) in (20,21,22,23) then 
																		 a.work_hours - datediff(mi,CONVERT(datetime,CONVERT(varchar, a.update_time, 23)+' '+ '20'+':'+'00'+':'+'00' ,120),a.update_time)   
																	else
																		 case
																			when 
																				 DATEPART(hour, update_time) in (0,1,2,3,4,5,6,7) then
																				 a.work_hours -	datediff(mi,CONVERT(datetime, convert(varchar,DATEADD(day, -1, CONVERT(varchar, a.update_time, 23)),23)+' '+'20:'+'00'+':'+'00',120),a.update_time)  
																			when DATEPART(hour, update_time) in (8,9,10,11,12,13,14,15,16,17,18,19) then	 
																				 a.work_hours - datediff(mi,CONVERT(datetime,CONVERT(varchar, a.update_time, 23)+' '+'08:'+'00'+':'+'00' ,120),a.update_time)
																		end 			
																end
														end 
												
												end as flag_smena2
												from(

											select code, state, update_time, DATEPART(day, update_time)as day, DATEPART(hour,  update_time)as hour,
											 
											datediff(mi,LAG(update_time) OVER(ORDER BY update_time, state), update_time) AS work_hours,
											case  
												when DATEPART(hour, update_time) in (20,21,22,23,0,1,2,3,4,5,6,7) then 1
												else 2
											end as smena,
											case
												 
												when DATEPART(hour, update_time) in (20,21,22,23) and DAY(DATEADD(Month, 1, update_time) - DAY(DATEADD(Month, 1, update_time)))>DATEPART(day, update_time)  then DATEPART(day, update_time)+1	 
												else
													case
														when DATEPART(hour, update_time) in (20,21,22,23) and DAY(DATEADD(Month, 1, update_time) - DAY(DATEADD(Month, 1, update_time)))=DATEPART(day, update_time)  then 1	
													else
														 DATEPART(day, update_time)
													end 
											end as day_right  
											 
											FROM io_data where code=".$request[$i] ." and UPDATE_TIME >= '" . $dateStart . "' AND 
																		UPDATE_TIME <= '" . $dateEnd . "'".")as a) as b) as c) as d) as e 
											where state=0 group by  e.smena, e.day_right";  
								 
											 
								 
											 
											 
						
	
			  //echo $qry;
			
			$svg_objects_query = $connect['scada_db']->query($qry);
			$answer = array();
			#$answer_ = array();
			while($row = $svg_objects_query->fetch(PDO::FETCH_ASSOC)){
				$answer[] = array (
							$request[$i]."_".$row['day_right']."_".$row['smena'],$row['update_ti'],
							$row['motohours'],$row['moto_prev'],$row['date_start']
							);   
				//array_push($answer_,$answer);
				#$answer=array();
			}
			
			$svg_objects_query = null;
			#if(count($answer)!=0){
			array_push($answerFinal, $answer); 
			#}else{
		#		array_push($answerFinal,$answer);	
	#		}
		}
		
		echo json_encode($answerFinal);
	}
	 
	if (isset($_POST['getTableData'])) {
        echo getTableData($_POST['getTableData'], $_POST['setDataStart'], $_POST['setDataEnd']);
	}
	if (isset($_POST['getRofarchiveData'])) {
        echo getRofarchiveData($_POST['getRofarchiveData'], $_POST['setDataStart'], $_POST['setDataEnd']);
	}
?>