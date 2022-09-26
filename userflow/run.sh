# superceeded by https://github.com/volodymyrss/cscs-bdms-tools/blob/bb7a09217b0e969b1a7fdd6312dc2caa378c5425/cscs-bdms-rucio.sh#L2

fn=cta1Mb-${file_id:?}
scope=${scope:-ctaarc}

set -x

function admin() {
	rucio-admin account add-attribute --key admin --value 1 root
	rucio-admin account list --filter "admin=1"

}


function scope() {
        rucio-admin scope add --scope ctaarc --account root
}



function protocol() {
	rucio-admin -v rse \
	    add-protocol \
	    CSCS-TMP \
	    --hostname posix \
	    --scheme posix \
	    --prefix "/tmp/rucio"  \
	    --impl rucio.rse.protocols.posix.Default \
	    --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}, "lan": {"read": 1, "write": 1, "delete": 1}}'
}

function upload() {
	dd if=/dev/random of=$fn bs=1M count=1
	rucio -v upload --scope $scope --rse CTA-SITE $fn
}


function ssh-add() {
	rucio-admin rse add ECOGIA-SSH-TMP || true
	rucio-admin rse add-protocol --hostname login01.astro.unige.ch --scheme scp --prefix /tmp/test-rucio --port 22 --impl rucio.rse.protocols.ssh.Default --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}, "lan": {"read": 1, "write": 1, "delete": 1}}' ECOGIA-SSH-TMP || true
	rucio-admin rse set-attribute --rse ECOGIA-SSH-TMP --key user --value savchenk || true
}

function create-rses() {
	rucio-admin rse add CTA-SITE || true
	rucio-admin -v rse \
	    add-protocol \
	    CTA-SITE \
	    --hostname posix \
	    --scheme posix \
	    --prefix "/tmp/rucio/cta-site"  \
	    --impl rucio.rse.protocols.posix.Default \
	    --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}, "lan": {"read": 1, "write": 1, "delete": 1}}'
	rucio-admin account set-limits root CTA-SITE 1073741824

        for dc_id in 0 1 2 3; do
            rucio-admin rse add CTA-DC-$dc_id || true
            rucio-admin -v rse \
                add-protocol \
                CTA-DC-$dc_id \
                --hostname posix \
                --scheme posix \
                --prefix "/tmp/rucio/cta-dc-$dc_id"  \
                --impl rucio.rse.protocols.posix.Default \
                --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}, "lan": {"read": 1, "write": 1, "delete": 1}}'
            rucio-admin account set-limits root CTA-DC-$dc_id 1073741824
            rucio-admin rse add-distance --distance 1 --ranking 1 CTA-SITE CTA-DC-$dc_id
        done

}

function add-rule() {
	# rucio add-dataset $scope:file1M
	rucio add-rule $scope:$fn 3 'CTA-DC-0|CTA-DC-1|CTA-DC-2|CTA-DC-3' || true
	rucio list-rules --file $scope:$fn
}

function add-rule-dataset() {
	rucio add-dataset $scope:ctaDS-A
	rucio add-dataset $scope:ctaDS-B
	rucio add-rule $scope:ctaDS-A 2 'CTA-DC-0|CTA-DC-1' || true
	rucio add-rule $scope:ctaDS-B 2 'CTA-DC-2|CTA-DC-3' || true
	rucio list-rules --dataset $scope:ctaDS-A
	rucio list-rules --dataset $scope:ctaDS-B
}

function flow() {
    add-rule-dataset

    upload # alternatively, add-replica
    #add-rule # if not attaching to dataset

    rucio attach $scope:ctaDS-${file_id::1} $scope:$fn
    #rucio-judge-evaluator --run-once

    #rucio-conveyor-submitter --sleep-time 1 --run-once
     
    #while true; do
    #    rucio-conveyor-poller --sleep-time 1 --run-once | tee poller-log
    #    < poller-log grep -q DONE && break
    #done


    #rucio-conveyor-finisher --sleep-time 1 --run-once
    #rucio list-file-replicas $scope:$fn
    #rucio list-rules --file $scope:$fn

    #rucio-judge-cleaner --run-once # delete expired rules
}


$@

	#rucio-admin rse add-protocol --hostname login01.astro.unige.ch --scheme scp --prefix /tmp/test-rucio --port 22 --impl rucio.rse.protocols.ssh.Default --domain-json '{"wan": {"read": 1, "write": 1, "delete": 1, "third_party_copy": 1}, "lan": {"read": 1, "write": 1, "delete": 1}}' ECOGIA-SSH-TMP || true
	#rucio-admin rse set-attribute --rse ECOGIA-SSH-TMP --key user --value savchenk || true
