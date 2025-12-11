import { useState } from 'react';

import SyncTaskList from './SyncTaskList';
import SyncDetail from './SyncDetail';
import DataSync from './DataSync';

const App = () => {
  const [detail, setDetail] = useState(false);
  const [params, setParams] = useState({})
  return (
    <>
      {detail ? (
        <div>
          <SyncDetail setDetail={setDetail} params={params}/>
        </div>
      ) : (
      <div>
        <DataSync setDetail={setDetail} setParams={setParams}/>
        <div>
          <SyncTaskList />
        </div>
      </div>
      )} 
    </>
  );
};

export default App;
