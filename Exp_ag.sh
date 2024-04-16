#!/bin/bash

declare variables() {

exp_proc_rqst="https://dae-app.wrkld-asset-servicing-1-prod-1.k8s.wpc.ntrs.com/batchJob/expaq"
exp_batch_rqst="https://dae-app.wrkld-asset-servicing-1-prod-1.k8s.wpc.ntrs.com"
error_msg=""
batch_id=0
exp_proc_status=1
max_batch_attempt=3
cur_batch_attempt=0

notify_email="br23entrs.com na27@ntrs.com" 
notify_subject="PROD env got FAILED: EXP Insert Process (Data Analytics Engine)"
}

exp_process_request () {

 echo "service request => $exp_proc_rqst"
 local result=$(curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET "$exp_proc_rqst")
 echo "service response => $result"

 batch_id=$(echo "$result" | grep -o '"batchId": *[^,]*' | grep -o '[^:]*$')
 local status=$(echo "$result" | grep -o '"status": *[^,]*' | grep -o '[^:]*$')
 batch_id=${batch_id//\"/}
 echo "status => $status"
 echo "batch_id => $batch_id"

 if [$status == "200" ]; then
   echo "EXP Insert Process Request Completed"
   while [ $cur_batch_attempt -lt $max_batch_attempt ]
   do
     sleep 5m
     exp_batch_status_request
   done

 else
   error_msg=$result
 fi
}

exp_batch_status_request () {

 echo "service request => $exp_batch_rqat/$batch_id"
 local result=$(curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET "$exp_batch_rqst/$batch_id")
 echo "service response => $result"
 local batch_status=$(echo "$result" | grep -o '"batchStatus": *"[^"]*"' | grep -o '"[^"]*"$')
 batch_status=${batch_status//\"/}
 echo "batch_status => $batch_status"

 if [ $batch_status == "SUCCESS" ]; then
    echo "EXP Insert Process Batch Status Completed"
    cur_batch_attempt=$max_batch_attempt
    exp_proc_status=2
    
 elif [ $batch_status == "ERROR" ]; then
    echo "EXP Insert Process Batch Status Failed"
    error_msg="EXP Insert Process Batch Processing Failed for Batch Id $batch_id"
    cur_batch_attempt=$max_batch_attempt
 else
    cur_batch_attempt=$(($cur_batch_attempt+1))
    echo "wait attempt => $cur_batch_attempt"
    error_msg=$result

 fi
}

exp_process_closure() {

 rc=1
 if [ $exp_proc_status -eq 2]; then
    echo "EXP Insert Process Completed"
    rc=0
 else
    echo "EXP Insert Process Failed"
    echo "See Ctrl M job for more details $error_msg" |mail -s "$notify_subject" "$notify_email"
 fi
 exit $rc

}

declare_variables
exp_process_request
exp_process_closure
