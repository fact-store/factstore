import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export let options = {
    vus: 10,           // virtual users
    duration: '30s',   // test duration
    thresholds: {
        http_req_duration: ['p(90)<500'], // 90% < 500ms
    },
};

export default function () {
    const url = 'http://localhost:8081/api/v1/fact-store/my-fact-store/facts/append';

    const payload = JSON.stringify({
        idempotencyKey: uuidv4(),
                                   facts: [
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                   vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                   id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                   vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                   id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                   vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                   id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       },
                                       {
                                           id: uuidv4(),
                                   type: 'UserCreated',
                                   subjectRef: {
                                       type: 'user',
                                       id: `user-${__VU}`, // per-VU subject
                                   },
                                   payload: {
                                       // "Hello world" base64-encoded
                                       data: 'SGVsbG8gd29ybGQ='
                                   },
                                   metadata: {
                                   },
                                   tags: {
                                       env: 'local',
                                       vu: String(__VU)
                                   }
                                       }

                                   ]
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'status is 200': (r) => r.status === 200,
    });
}
