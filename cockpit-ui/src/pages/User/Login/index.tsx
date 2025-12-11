import React, { useEffect, useState } from "react";
import { LOGIN_MENU, LOGIN_MENU_MAP } from "./config";
import "./index.less";
import egTwoContent from "./img/eg3-content.png";
import { Carousel } from "antd";
// import "./style-addition.less";

const carouselList = [
  <div key="2">
    <div className="carousel-eg-ctr carousel-eg-ctr-two">
      <img className="carousel-eg-ctr-two-img img-one" src={egTwoContent} />
      <div className="carousel-eg-ctr-two-desc desc-two">
      一个集数据采集、存储、处理与分析于一体的全方位智能数据平台
      </div>
    </div>
  </div>,
  // <div key="1">
  //   <div className="carousel-eg-ctr carousel-eg-ctr-one">
  //     <img className="carousel-eg-ctr-one-img img-one" src={egOneTitle} />
  //     <div className="carousel-eg-ctr-one-desc desc-one">可能是北半球最简单易用的 Kafka 管控平台</div>
  //     <img className="carousel-eg-ctr-one-img img-two" src={egOneContent} />
  //   </div>
  // </div>,
];

export const Login: React.FC<any> = () => {

  const [selectedKeys, setSelectedKeys] = useState([LOGIN_MENU[0].key]);

  const renderContent = () => {
    return (
      LOGIN_MENU_MAP.get(selectedKeys[0])?.render(handleMenuClick) ||
      LOGIN_MENU_MAP.get(LOGIN_MENU[0].key)?.render(handleMenuClick)
    );
  };

  const handleMenuClick = (e: string) => {
    setSelectedKeys([e]);
    window.location.hash = e;
  };

  return (
    <div className="login-page">
      <div className="login-page-left">
        <Carousel autoplay={true} autoplaySpeed={5000}>
          {carouselList}
        </Carousel>
      </div>
      <div className="login-page-right">
        <div className="login-page-right-content">
          <div className="login-page-right-content-title">
            {/* <img className="logo" src={Logo} /> */}
            <div className="desc">Cockpit 大数据平台</div>
          </div>
          <div className="login-page-right-content-content">
            {renderContent()}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
