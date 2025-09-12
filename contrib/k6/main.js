import http from 'k6/http';

export const options = {
  // define scenarios
  scenarios: {
    // arbitrary name of scenario
    average_load: {
      executor: 'ramping-vus',
      stages: [
        // ramp up to average load of 20 virtual users
        { duration: '10s', target: 50 },
        // maintain load
        { duration: '50s', target: 80 },
        // ramp down to zero
        { duration: '5s', target: 0 },
      ],
    },
  },
};


export default function () {
  const url = 'http://0.0.0.0:8000/v1/test/test';
  const res = http.get(url);
	// console.log(res)
}
