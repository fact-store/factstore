import http from 'k6/http';
import { check } from 'k6';

export let options = {
    stages: [
        { duration: '30s', target: 200 },  // ramp to 200 VUs
        { duration: '1m', target: 200 },
        { duration: '30s', target: 500 },  // ramp to 500 VUs
        { duration: '1m', target: 500 },
        { duration: '30s', target: 1000 }, // ramp to 1000 VUs
        { duration: '2m', target: 1000 },
        { duration: '30s', target: 0 },    // ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<100'], // tighten SLA: 95% < 100ms
        http_req_failed: ['rate<0.01'],   // <1% failures allowed
    },
};

export default function () {
    const res = http.post('http://localhost:8080/test');
    check(res, {
        'status is 204': (r) => r.status === 204,
    });
}
