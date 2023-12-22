self.onmessage = async (e) => {
    console.log('i am inside worker' + JSON.stringify(e));
    self.close();
  };