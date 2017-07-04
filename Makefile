test_Add_edges:
	python -m pytest insight_testsuite/test_add_edges/

test_anomaly_detection:
	cd insight_testsuite/ && bash run_tests.sh
